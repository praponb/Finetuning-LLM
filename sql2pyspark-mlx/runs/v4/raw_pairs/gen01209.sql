select
    id,
    salary,
    manager_id,
    case when job_title = 'Analyst' then 1 else 0 end as is_analyst
from employees
where (salary between 119000 and 139000 and dept_id = 19) or is_remote = true;
