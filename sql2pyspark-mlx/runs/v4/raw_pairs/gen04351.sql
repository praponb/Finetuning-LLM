select
    e.id,
    e.email,
    e.hire_date,
    e.is_remote,
    d.budget
from employees e
inner join departments d on e.dept_id = d.id
where d.dept_name is not null
