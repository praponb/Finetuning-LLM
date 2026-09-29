select
    e.manager_id,
    e.job_title,
    d.dept_name,
    d.location
from employees e
right join departments d on e.dept_id = d.id;
