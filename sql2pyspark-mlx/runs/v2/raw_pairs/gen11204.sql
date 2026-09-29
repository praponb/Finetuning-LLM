select
    e.email,
    e.dept_id,
    d.dept_name,
    d.location,
    d.budget
from employees e
inner join departments d on d.id = e.dept_id;
