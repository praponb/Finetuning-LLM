select
    emp.job_title as employee_job_title,
    emp.email as employee_email,
    emp.salary as employee_salary,
    mgr.full_name as manager_full_name,
    concat(mgr.level, ' - ', mgr.email) as level_email
from employees emp
inner join employees mgr on emp.manager_id = mgr.id;
