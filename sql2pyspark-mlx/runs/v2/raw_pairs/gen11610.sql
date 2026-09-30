select
    d.id,
    d.location,
    d.budget,
    d.dept_name
from departments d
where not exists (select 1 from employees e where e.dept_id = d.id and e.job_title in ('Manager', 'Engineer', 'Analyst'))
order by dept_name desc, id
limit 5
