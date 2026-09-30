select
    e.id,
    e.level as employee_level,
    e.email as employee_email,
    m.salary as manager_salary
from employees as e
inner join employees as m on e.manager_id = m.id
where e.manager_id is not null
