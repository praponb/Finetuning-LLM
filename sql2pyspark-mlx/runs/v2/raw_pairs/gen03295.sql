select
    e.id,
    e.is_remote,
    d.budget,
    d.dept_name
from employees e
right outer join departments d on d.id = e.dept_id
where e.email like '%@gmail.com' and d.budget is not null;
