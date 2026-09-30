select
    e.id,
    e.full_name,
    e.salary,
    e.manager_id
from employees e
where e.dept_id in (select id from departments where location in ('London', 'Singapore'));
