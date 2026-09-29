select
    e.id,
    e.salary as employee_salary,
    m.manager_id as manager_manager_id,
    m.salary as manager_salary
from employees as e
left join employees as m on e.manager_id = m.id
order by manager_salary desc, id;
