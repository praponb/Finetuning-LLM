select e.dept_id, e.salary, d.dept_name
from employees e
inner join departments d on d.id = e.dept_id
where d.dept_name = 'HR'
