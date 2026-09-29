select
    e.id,
    e.salary,
    e.dept_id,
    d.dept_name,
    d.location
from employees e
left outer join departments d on d.id = e.dept_id
where e.manager_id is not null
order by 2 desc, dept_name desc
