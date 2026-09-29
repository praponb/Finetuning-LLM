select
    e.id,
    e.hire_date as employee_hire_date,
    m.manager_id as manager_manager_id,
    m.full_name as manager_full_name,
    m.email as manager_email
from employees e
join employees m on e.manager_id = m.id
order by manager_full_name, m.manager_id desc
