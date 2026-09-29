select
    id,
    location,
    budget,
    dept_name,
    first_value(dept_name) over (partition by location order by budget, id) as first_dept_name
from departments
where dept_name in ('Finance', 'Engineering');
