select
    e.id,
    e.manager_id as employee_manager_id,
    e.email as employee_email,
    m.email as manager_email,
    m.manager_id as manager_manager_id
from employees e
inner join employees m on e.manager_id = m.id
where e.level in ('L4', 'L1', 'L5');
