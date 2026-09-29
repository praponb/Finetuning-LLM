with src as (
    select *
    from employees
    where level in ('L2', 'L5')
),
agg as (
    select job_title, case when salary >= 124000 then 'high' when salary >= 56000 then 'mid' else 'low' end as salary_bucket, avg(salary) as avg_salary
    from src
    group by job_title, case when salary >= 124000 then 'high' when salary >= 56000 then 'mid' else 'low' end
)
select job_title, salary_bucket, avg_salary
from agg;
