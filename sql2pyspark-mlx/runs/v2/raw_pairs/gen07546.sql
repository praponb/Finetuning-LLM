select
    id,
    job_title,
    salary,
    hire_date
from (
    select *, row_number() over (partition by job_title order by salary desc, id) as rn
    from employees
    where is_remote
) as r
where rn <= 2
order by job_title;
