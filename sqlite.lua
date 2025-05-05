local ffi = require "ffi"
local buffer = require "string.buffer"

-- TODO: if we want to accommodate callers who provide their own vtables, we could try to reflect
-- these types from their signatures (e.g. if m3 wants to rename `sqlite3_*` to avoid conflicts)
ffi.cdef [[
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
]]

-- find C library, in priority order:
--   1. script argument
--   2. env LIBSQLITE
--   3. ffi.C
--   4. "sqlite3"
local C = (function(clib)
	local function initsym(lib) return lib.sqlite3_initialize end
	local function checkinitsym(lib) local ok,v = pcall(initsym, lib) return ok and v end
	-- did caller provide a vtable? then we don't need to define the symbols.
	if checkinitsym(clib) then return clib end
	ffi.cdef [[
int sqlite3_initialize(void);
int sqlite3_open(const char *, sqlite3 **);
int sqlite3_close_v2(sqlite3 *);
int sqlite3_exec(sqlite3 *, const char *, int (*)(void *, int, char **, char **), void *, char **);
int sqlite3_prepare_v2(sqlite3 *, const char *, int, sqlite3_stmt **, const char **);
int sqlite3_step(sqlite3_stmt *);
int sqlite3_reset(sqlite3_stmt *);
int sqlite3_finalize(sqlite3_stmt *);
int sqlite3_bind_double(sqlite3_stmt *, int, double);
int sqlite3_bind_int64(sqlite3_stmt *, int, int64_t);
int sqlite3_bind_null(sqlite3_stmt *, int);
int sqlite3_bind_text(sqlite3_stmt *, int, const char *, int, void(*)(void*));
double sqlite3_column_double(sqlite3_stmt *, int);
int sqlite3_column_int(sqlite3_stmt *, int);
const char *sqlite3_column_text(sqlite3_stmt *, int);
int sqlite3_column_type(sqlite3_stmt *, int);
int sqlite3_column_count(sqlite3_stmt *);
const char *sqlite3_column_name(sqlite3_stmt *, int);
const char *sqlite3_sql(sqlite3_stmt *);
int sqlite3_bind_parameter_count(sqlite3_stmt *);
int sqlite3_bind_parameter_index(sqlite3_stmt *, const char *);
sqlite3 *sqlite3_db_handle(sqlite3_stmt *);
const char *sqlite3_errstr(int);
const char *sqlite3_errmsg(sqlite3 *);
const char *sqlite3_libversion(void);
	]]
	if checkinitsym(clib) then return clib end
	local libsqlite = os.getenv("LIBSQLITE")
	if libsqlite then return ffi.load(libsqlite) end
	if checkinitsym(ffi.C) then return ffi.C end
	return ffi.load("sqlite3")
end)(...)

-- NOTE: sqlite3_shutdown is never called, maybe expose it?
C.sqlite3_initialize()

local SQLITE_TRANSIENT = ffi.cast("void *", -1)
local SQLITE_ROW  = 100
local SQLITE_DONE = 101
local SQLITE_INTEGER = 1
local SQLITE_FLOAT = 2
local SQLITE_TEXT = 3

local function version()
	return ffi.string(C.sqlite3_libversion())
end

---- Query builder -------------------------------------------------------------
-- see: https://www.sqlite.org/lang.html

local function sql(fragment, ...)
	return {[0]=fragment, ...}
end

local function issql(x)
	return type(x) == "table" and x[0]
end

local function escape(str)
	-- TODO
	return str
end

local function sqlerr(sql, err)
	-- TODO better message
	error(err, 2)
end

local function flatten_sql(t,k,x)
	local l = t[k]
	if not l then
		l = {}
		t[k] = l
	end
	for _,v in ipairs(x) do
		table.insert(l,v)
	end
end

local function flatten_aux(t,x)
	local sql = x[0]
	if sql then
		flatten_sql(t,sql,x)
	else
		for k,v in pairs(x) do
			if type(k) == "number" then
				flatten_aux(t,v)
			else
				if type(v) ~= "table" then
					v = {v}
				end
				flatten_sql(t,k,v)
			end
		end
	end
end

local function putlist(buf, values, sep)
	for i,v in ipairs(values) do
		if i>1 then buf:put(sep or ",") end
		buf:put(v)
	end
end

local function flatten(x)
	local t = {}
	flatten_aux(t,x)
	return t
end

local sql_stmt

local function sql_select_expr(buf, sql)
	if type(sql) == "string" then
		buf:put(sql)
	else
		error("TODO")
	end
end

local function sql_select_stmt(buf, sql)
	if sql.WITH then
		buf:put(" WITH ")
		for i,v in ipairs(sql.WITH) do
			if i>1 then buf:put(",") end
			buf:put(v.name, " AS (") -- TODO: handle columns
			sql_stmt(buf, v.cte)
			buf:put(")")
		end
	end
	buf:put(" SELECT ")
	for i,v in ipairs(sql.SELECT) do
		if i>1 then buf:put(",") end
		if issql(v) == "AS" then
			sql_select_expr(buf, v[1])
			buf:put(" AS ", v[2])
		else
			sql_select_expr(buf, v)
		end
	end
	if sql.FROM then
		buf:put(" FROM ")
		putlist(buf, sql.FROM)
	end
	if sql.WHERE then
		buf:put(" WHERE ")
		putlist(buf, sql.WHERE, " AND ")
	end
	local compound = sql.UNION and "UNION"
		or sql["UNION ALL"] and "UNION ALL"
		or sql.INTERSECT and "INTERSECT"
		or sql.EXCEPT and "EXCEPT"
	if compound then
		buf:put(" ", compound, " ")
		sql_stmt(buf, sql[compound])
	end
	if sql["ORDER BY"] then
		buf:put(" ORDER BY ")
		for i,v in ipairs(sql["ORDER BY"]) do
			if i>1 then buf:put(",") end
			sql_select_expr(buf, v)
		end
	end
end

local function sql_insert_stmt(buf, sql)
	buf:put("INSERT INTO ", sql.INSERT[1])
	if sql.VALUES then
		buf:put("(")
		for i,v in ipairs(sql.VALUES) do
			if i>1 then buf:put(",") end
			buf:put(v.col)
		end
		buf:put(") VALUES (")
		for i,v in ipairs(sql.VALUES) do
			if i>1 then buf:put(",") end
			buf:put(v.value)
		end
		buf:put(")")
	end
end

sql_stmt = function(buf, sql)
	sql = flatten(sql)
	if sql.SELECT then
		return sql_select_stmt(buf, sql)
	elseif sql.INSERT then
		return sql_insert_stmt(buf, sql)
	else
		for k,v in pairs(sql) do
			print(k,v)
		end
		sqlerr(sql, "expected top-level statement")
	end
end

local function stringify(sql)
	if type(sql) == "table" then
		local buf = buffer.new()
		sql_stmt(buf, sql)
		sql = buf
	end
	return tostring(sql)
end

---- C API ---------------------------------------------------------------------

local function throw(x)
	local isstmt = ffi.istype("sqlite3_stmt *", x)
	local handle
	if isstmt then
		handle = C.sqlite3_db_handle(x)
	else
		handle = x
	end
	local err = ffi.string(C.sqlite3_errmsg(handle))
	if isstmt then
		err = string.format("%s\n\tSQL: %s", err, x:sql())
	end
	error(err, 0)
end

local function check(x, r)
	if r ~= 0 then
		throw(x)
	end
end

local function stmt_bind(stmt, i, v)
	local ty = type(v)
	if ty == "nil" then
		check(stmt, C.sqlite3_bind_null(stmt, i))
	elseif ty == "number" then
		check(stmt, C.sqlite3_bind_double(stmt, i, v))
	elseif ty == "cdata" then
		check(stmt, C.sqlite3_bind_int64(stmt, i, v))
	elseif ty == "string" then
		check(stmt, C.sqlite3_bind_text(stmt, i, v, #v, SQLITE_TRANSIENT))
	elseif ty == "table" then
		for idx,value in pairs(v) do
			if type(idx) == "string" then
				idx = C.sqlite3_bind_parameter_index(stmt, idx)
				if idx == 0 then goto continue end
			end
			stmt_bind(stmt, idx, value)
			::continue::
		end
	else
		error(string.format("can't bind: %s", v))
	end
	return stmt
end

-- *insert cs grad meme*
local function stmt_bindargs(stmt, ...)
	local n = select("#", ...)
	if n == 0 then
	elseif n == 1 then
		local v1 = ...
		stmt_bind(stmt, 1, v1)
	elseif n == 2 then
		local v1, v2 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2)
	elseif n == 3 then
		local v1, v2, v3 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3)
	elseif n == 4 then
		local v1, v2, v3, v4 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
	elseif n == 5 then
		local v1, v2, v3, v4, v5 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
		stmt_bind(stmt, 5, v5)
	elseif n == 6 then
		local v1, v2, v3, v4, v5, v6 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
		stmt_bind(stmt, 5, v5) stmt_bind(stmt, 6, v6)
	elseif n == 7 then
		local v1, v2, v3, v4, v5, v6, v7 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
		stmt_bind(stmt, 5, v5) stmt_bind(stmt, 6, v6) stmt_bind(stmt, 7, v7)
	elseif n == 8 then
		local v1, v2, v3, v4, v5, v6, v7, v8 = ...
		stmt_bind(stmt, 1, v1) stmt_bind(stmt, 2, v2) stmt_bind(stmt, 3, v3) stmt_bind(stmt, 4, v4)
		stmt_bind(stmt, 5, v5) stmt_bind(stmt, 6, v6) stmt_bind(stmt, 7, v7) stmt_bind(stmt, 8, v8)
	else
		stmt_bind(stmt, nil, {...})
	end
	return stmt
end

local function stmt_reset(stmt)
	check(stmt, C.sqlite3_reset(stmt))
end

local function stmt_dofinalize(stmt)
	check(stmt, C.sqlite3_finalize(stmt))
end

local function stmt_finalize(stmt)
	return stmt_dofinalize(ffi.gc(stmt, nil))
end

local function stmt_gc(stmt)
	return ffi.gc(stmt, stmt_dofinalize)
end

local function stmt_step(stmt)
	local s = C.sqlite3_step(stmt)
	if s == SQLITE_ROW then
		return stmt
	elseif s == SQLITE_DONE then
		stmt_reset(stmt)
	else
		throw(stmt)
	end
end

local function stmt_rows(stmt, ...)
	stmt_bindargs(stmt, ...)
	return stmt_step, stmt
end

local function stmt_exec(stmt, ...)
	stmt_bindargs(stmt, ...)
	if stmt_step(stmt) then
		stmt_reset(stmt)
	end -- else: stmt_step() already called reset
end

local function stmt_text(stmt, i)
	local p = C.sqlite3_column_text(stmt, i)
	if p ~= nil then return ffi.string(p) end
end

local function stmt_col(stmt, i)
	local ty = C.sqlite3_column_type(stmt, i)
	if ty == SQLITE_INTEGER then
		return (C.sqlite3_column_int(stmt, i))
	elseif ty == SQLITE_FLOAT then
		return (C.sqlite3_column_double(stmt, i))
	elseif ty == SQLITE_TEXT then
		return stmt_text(stmt, i)
	end
end

local function stmt_name(stmt, i)
	local name = C.sqlite3_column_name(stmt, i)
	if name ~= nil then
		return ffi.string(name)
	end
end

local function stmt_row(stmt, names)
	local row = {}
	if names then
		for i=0, C.sqlite3_column_count(stmt)-1 do
			row[stmt_name(stmt, i)] = stmt_col(stmt, i)
		end
	else
		for i=1, C.sqlite3_column_count(stmt) do
			row[i] = stmt_col(stmt, i-1)
		end
	end
	return row
end

local function stmt_unpack1(stmt, i, n)
	if i<n then
		return stmt_col(stmt, i), stmt_unpack1(stmt, i+1, n)
	end
end

local function stmt_unpack(stmt)
	return stmt_unpack1(stmt, 0, C.sqlite3_column_count(stmt))
end

local function stmt_sql(stmt)
	return ffi.string(C.sqlite3_sql(stmt))
end

ffi.metatype("sqlite3_stmt", {
	__index = {
		bind       = stmt_bind,
		bindargs   = stmt_bindargs,
		paramcount = C.sqlite3_bind_parameter_count,
		colcount   = C.sqlite3_column_count,
		reset      = stmt_reset,
		finalize   = stmt_finalize,
		gc         = stmt_gc,
		step       = stmt_step,
		rows       = stmt_rows,
		row        = stmt_row,
		unpack     = stmt_unpack,
		exec       = stmt_exec,
		double     = C.sqlite3_column_double,
		int        = C.sqlite3_column_int,
		text       = stmt_text,
		col        = stmt_col,
		name       = stmt_name,
		sql        = stmt_sql
	}
})

local function open(url)
	local buf = ffi.new("sqlite3 *[1]")
	local err = C.sqlite3_open(url, buf)
	if err ~= 0 then
		error(ffi.string(C.sqlite3_errstr(err)))
	end
	return buf[0]
end

local function sqlite3_doclose(conn)
	check(conn, C.sqlite3_close_v2(conn))
end

local function sqlite3_close(conn)
	return sqlite3_doclose(ffi.gc(conn, nil))
end

local function sqlite3_gc(conn)
	return ffi.gc(conn, sqlite3_doclose)
end

local function sqlite3_prepare(conn, sql)
	sql = stringify(sql)
	local buf = ffi.new("sqlite3_stmt *[1]")
	check(conn, C.sqlite3_prepare_v2(conn, sql, #sql, buf, nil))
	return buf[0]
end

local function sqlite3_rows(conn, sql, ...)
	return sqlite3_prepare(conn, sql):gc():rows(...)
end

local function sqlite3_row(conn, sql, ...)
	local stmt = sqlite3_prepare(conn, sql):gc()
	stmt:bindargs(...)
	stmt:step()
	local row = stmt:row()
	stmt:finalize()
	return row
end

local function sqlite3_exec(conn, sql, ...)
	local stmt = sqlite3_prepare(conn, sql):gc()
	stmt:exec(...)
	stmt:finalize()
end

local function sqlite3_execscript(conn, sql)
	check(conn, C.sqlite3_exec(conn, stringify(sql), nil, nil, nil))
end

ffi.metatype("sqlite3", {
	__index = {
		close      = sqlite3_close,
		gc         = sqlite3_gc,
		prepare    = sqlite3_prepare,
		rows       = sqlite3_rows,
		row        = sqlite3_row,
		exec       = sqlite3_exec,
		execscript = sqlite3_execscript
	}
})

---- Reflection ----------------------------------------------------------------

local function reflect__index(self, key)
	local field = getmetatable(self).fields[key]
	if field then
		local value = field(self, key)
		rawset(self, key, value)
		return value
	end
end

local function reftab_columns(tab)
	local columns = {}
	for row in tab.conn:rows(string.format("PRAGMA table_xinfo(%s)", tab.name)) do
		columns[row:text(1)] = {
			nullable = row:int(3) == 0,
			pk = row:int(5) == 1
		}
	end
	return columns
end

local function reftab_fks(tab)
	local fks = {}
	local cur
	for row in tab.conn:rows(string.format("PRAGMA foreign_key_list(%s)", tab.name)) do
		if row:int(1) == 0 then
			table.insert(fks, cur)
			cur = { table=row:text(2), columns={} }
		end
		cur.columns[row:text(3)] = row:text(4)
	end
	table.insert(fks, cur)
	return fks
end

local reftab_mt = {
	fields = {
		columns      = reftab_columns,
		foreign_keys = reftab_fks
	},
	__index = reflect__index
}

local function reflect_tables(refl)
	local tables = {}
	for row in refl.conn:rows("PRAGMA table_list") do
		-- prefer first occurrence if the same table name appears in multiple schemas.
		-- this matches sqlite behavior.
		local name = row:text(1)
		if not tables[name] then
			tables[name] = setmetatable({conn=refl.conn, name=name}, reftab_mt)
		end
	end
	return tables
end

local function reflect_databases(refl)
	local databases = {}
	for row in refl.conn:rows("PRAGMA database_list") do
		local db = {}
		local file = row:text(2)
		if file ~= "" then db.file = file end
		databases[row:text(1)] = db
	end
	return databases
end

local reflect_mt = {
	fields = {
		tables = reflect_tables,
		databases = reflect_databases
	},
	__index = reflect__index
}

local function reflect(conn)
	return setmetatable({conn=conn}, reflect_mt)
end

--------------------------------------------------------------------------------

return {
	sql       = sql,
	stringify = stringify,
	escape    = escape,
	open      = open,
	reflect   = reflect,
	version   = version
}
