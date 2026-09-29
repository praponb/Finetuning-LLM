select
    id,
    dept_name,
    location,
    budget,
    substring(dept_name, 1, 2) as dept_name_prefix
from departments
where location in ('Singapore', 'New York') and location in ('Chiang Mai', 'Singapore', 'New York') and location = 'Berlin'
order by location
