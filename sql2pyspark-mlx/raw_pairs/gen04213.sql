select
    e.id,
    e.salary as employee_salary,
    e.hire_date as employee_hire_date,
    e.full_name as employee_full_name,
    m.manager_id as manager_manager_id,
    m.salary as manager_salary,
    m.full_name as manager_full_name
from employees e
join employees m on e.manager_id = m.id
where e.manager_id = 21 and e.full_name is not null
