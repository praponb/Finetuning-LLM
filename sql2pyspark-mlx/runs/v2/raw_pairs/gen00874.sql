select
    d.id,
    d.budget,
    d.location,
    d.dept_name
from departments d
where not exists (select 1 from employees e where e.dept_id = d.id)
order by d.dept_name asc, id asc
