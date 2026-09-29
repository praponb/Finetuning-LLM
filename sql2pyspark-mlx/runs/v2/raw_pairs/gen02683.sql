select e.id, e.dept_id
from employees e
where e.dept_id in (select id from departments where budget > 3563000);
