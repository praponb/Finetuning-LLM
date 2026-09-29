select
    emp.id,
    emp.job_title as employee_job_title,
    emp.full_name as employee_full_name,
    mgr.salary as manager_salary,
    mgr.email as manager_email,
    mgr.is_remote as manager_is_remote
from employees as emp
left join employees as mgr on emp.manager_id = mgr.id
