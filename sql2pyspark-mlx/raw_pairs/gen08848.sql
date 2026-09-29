select *, trim(location) as clean_location, lower(dept_name) as dept_name_lower from departments where budget >= 4017000 and dept_name is null order by id desc;
