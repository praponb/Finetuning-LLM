select d.id, d.budget
from departments d
where d.dept_name in ('Finance', 'Sales') and d.id in (select dept_id from employees)
