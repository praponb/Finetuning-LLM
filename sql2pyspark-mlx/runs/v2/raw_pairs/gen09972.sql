select
    e.id,
    e.job_title,
    d.budget,
    d.dept_name,
    d.location
from employees e
left outer join departments d on e.dept_id = d.id
where e.level is null;
