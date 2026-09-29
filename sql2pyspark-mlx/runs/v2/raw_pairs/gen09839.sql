select e.id, e.dept_id, d.dept_name
from employees e
right join departments d on d.id = e.dept_id;
