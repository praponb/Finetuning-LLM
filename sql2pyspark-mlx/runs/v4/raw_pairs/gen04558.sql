select
    d.dept_name,
    d.location,
    d.budget,
    e.is_remote,
    e.salary,
    e.dept_id
from departments d
left join employees e on d.id = e.dept_id
where e.job_title in ('Designer', 'Intern') and e.manager_id = 1
order by dept_id desc
