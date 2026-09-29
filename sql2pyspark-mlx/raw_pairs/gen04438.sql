select id, budget, concat_ws('-', dept_name, location) as dept_name_location_key from departments where dept_name != 'HR' and location in ('Chiang Mai', 'Tokyo') and budget < 1276000
