select id, job_title, salary
from (
    select
        id,
        job_title,
        salary,
        row_number() over (partition by job_title order by salary desc, id) as row_num
    from employees
    where dept_id in (1, 13, 30)
) as r
where row_num <= 3
order by job_title desc;
