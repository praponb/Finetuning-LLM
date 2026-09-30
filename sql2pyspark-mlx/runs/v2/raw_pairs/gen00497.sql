select
    e.level as employee_level,
    e.manager_id as employee_manager_id,
    e.dept_id as employee_dept_id,
    m.dept_id as manager_dept_id,
    m.email as manager_email,
    m.hire_date as manager_hire_date
from employees e
left join employees m on e.manager_id = m.id
where m.email is not null
order by m.email;
