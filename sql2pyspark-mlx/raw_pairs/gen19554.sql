select
    e.salary,
    e.job_title,
    e.full_name,
    d.location,
    d.budget
from employees e
join departments d on e.dept_id = d.id;
