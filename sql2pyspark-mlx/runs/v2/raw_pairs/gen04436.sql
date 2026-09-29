select
    id,
    job_title,
    salary,
    full_name,
    last_value(full_name) over (partition by job_title order by salary desc, id rows between unbounded preceding and unbounded following) as last_full_name
from employees
where is_remote = true;
