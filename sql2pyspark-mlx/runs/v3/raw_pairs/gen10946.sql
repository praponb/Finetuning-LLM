select
    departments.id,
    departments.location,
    departments.budget,
    departments.dept_name
from departments
where departments.location in ('London', 'Singapore', 'Berlin') and departments.id in (select dept_id from employees where email like '%@yahoo.com')
order by dept_name, id
limit 10;
