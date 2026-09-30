select id, location, dept_name from departments where location is not null union all select id, location, dept_name from departments where location = 'Tokyo' order by dept_name;
