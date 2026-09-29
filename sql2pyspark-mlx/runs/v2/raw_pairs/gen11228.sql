select
    e.id,
    e.is_remote,
    e.job_title,
    d.location,
    d.dept_name
from employees e
inner join departments d on e.dept_id = d.id
order by id desc
limit 50
