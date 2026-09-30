select
    location,
    dept_name,
    budget,
    concat(dept_name, ', ', location) as dept_name_location
from departments;
