select
    budget,
    dept_name,
    location,
    upper(location) as location_upper,
    upper(dept_name) as dept_name_upper
from departments
where location = 'Bangkok'
order by dept_name;
