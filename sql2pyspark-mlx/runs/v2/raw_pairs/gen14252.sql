select
    e.level as employee_level,
    e.is_remote as employee_is_remote,
    e.hire_date as employee_hire_date,
    m.manager_id as manager_manager_id,
    m.hire_date as manager_hire_date
from employees e
left outer join employees m on e.manager_id = m.id
