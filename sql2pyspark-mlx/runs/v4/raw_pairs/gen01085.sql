with top_employees as (
    select
        id,
        job_title,
        salary,
        row_number() over (partition by job_title order by salary desc, id) as rnk
    from employees
)
select id, job_title, salary
from top_employees
where rnk <= 3;
