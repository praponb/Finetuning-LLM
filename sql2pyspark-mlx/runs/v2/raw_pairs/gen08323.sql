select
    emp.id,
    emp.job_title employee_job_title,
    emp.is_remote employee_is_remote,
    mgr.full_name manager_full_name,
    mgr.email manager_email,
    substring(emp.level, 1, 4) level_prefix
from employees emp
inner join employees mgr on emp.manager_id = mgr.id;
