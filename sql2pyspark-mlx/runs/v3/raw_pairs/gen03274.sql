select id, job_title, salary
from (
    select
        id,
        job_title,
        salary,
        row_number() over (partition by job_title order by salary, id) as row_num
    from employees
    where level in ('L4', 'L1')
) as r
where row_num <= 3
order by 1 desc
