select
    e.id,
    e.full_name as employee_full_name,
    e.dept_id as employee_dept_id,
    e.level as employee_level,
    m.email as manager_email
from employees e
left join employees m on e.manager_id = m.id
where e.email is not null
order by employee_full_name, employee_level
