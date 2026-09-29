select
    id,
    job_title,
    salary,
    full_name
from (
    select
        id,
        job_title,
        salary,
        full_name,
        rank() over (partition by job_title order by salary desc) pos
    from employees
) ranked
where pos <= 2;
