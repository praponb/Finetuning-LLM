select d.id, d.dept_name, d.budget
from departments d
where not exists (select 1 from employees e where e.dept_id = d.id and e.full_name is not null)
order by id desc
