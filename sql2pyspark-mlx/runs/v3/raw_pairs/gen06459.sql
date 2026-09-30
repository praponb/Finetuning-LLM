select
    e.id,
    e.salary as employee_salary,
    e.job_title as employee_job_title,
    e.hire_date as employee_hire_date,
    m.email as manager_email,
    m.manager_id as manager_manager_id
from employees e
left outer join employees m on e.manager_id = m.id
where m.level in ('L5', 'L2')
order by manager_email asc, employee_job_title asc, id asc
limit 1;
