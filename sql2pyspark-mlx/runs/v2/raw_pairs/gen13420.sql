select
    emp.id,
    emp.salary as employee_salary,
    emp.job_title as employee_job_title,
    mgr.is_remote as manager_is_remote,
    upper(trim(emp.full_name)) as full_name_normalized
from employees as emp
left join employees as mgr on emp.manager_id = mgr.id
where length(emp.email) < 6;
