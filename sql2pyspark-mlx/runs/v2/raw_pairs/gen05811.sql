select
    e.id,
    e.manager_id as employee_manager_id,
    m.level as manager_level,
    m.email as manager_email
from employees e
left outer join employees m on e.manager_id = m.id
order by employee_manager_id desc, m.email;
