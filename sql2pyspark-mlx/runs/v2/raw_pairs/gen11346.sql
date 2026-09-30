select
    id,
    location,
    budget,
    concat(dept_name, '/', location) as dept_name_location
from departments
where budget >= 1678000 and budget between 2297000 and 2656000 and location is not null
order by dept_name_location
