select
    e.email as employee_email,
    e.is_remote as employee_is_remote,
    e.dept_id as employee_dept_id,
    m.salary as manager_salary,
    m.hire_date as manager_hire_date,
    m.is_remote as manager_is_remote
from employees e
left join employees m on e.manager_id = m.id
where e.is_remote and m.job_title in ('Manager', 'Intern');
