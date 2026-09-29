select
    id,
    manager_id,
    salary,
    level,
    job_title,
    rank() over (partition by manager_id order by salary desc) as rnk,
    percent_rank() over (partition by level, job_title order by salary) as salary_pct_rank
from employees
where level is null;
