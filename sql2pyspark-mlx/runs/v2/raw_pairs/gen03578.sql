select d.id, d.budget
from departments d
where exists (select 1 from employees e where e.dept_id = d.id)
