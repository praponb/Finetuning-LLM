select
    d.id,
    d.dept_name,
    d.location,
    d.budget
from departments d
where d.dept_name = 'Finance' and not exists (select 1 from employees e where e.dept_id = d.id and e.salary < 140000)
