select
    e.id,
    e.level as employee_level,
    e.full_name as employee_full_name,
    m.dept_id as manager_dept_id,
    m.hire_date as manager_hire_date
from employees e
left join employees m on e.manager_id = m.id
where e.full_name like 'Em%' and m.hire_date >= '2024-10-01';
