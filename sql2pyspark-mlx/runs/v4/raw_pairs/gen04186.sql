select
    e.manager_id as employee_manager_id,
    e.hire_date as employee_hire_date,
    e.level as employee_level,
    m.salary as manager_salary
from employees e
left outer join employees m on e.manager_id = m.id
where lower(trim(m.email)) = 'carol@company.co' or m.job_title is null;
