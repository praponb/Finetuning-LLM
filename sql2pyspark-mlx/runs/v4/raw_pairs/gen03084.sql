select budget, location, concat_ws('-', dept_name, location) as dept_name_location_key from departments where dept_name = 'HR' and budget > 1492000 order by budget desc;
