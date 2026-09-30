select
    e.id,
    e.is_remote,
    e.salary,
    e.email,
    d.dept_name,
    d.location
from employees e
left join departments d on e.dept_id = d.id
where e.hire_date >= '2024-12-01' and d.dept_name = 'Finance'
order by dept_name desc;
