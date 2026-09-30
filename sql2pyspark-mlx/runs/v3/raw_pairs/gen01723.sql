with top_employees as (
    select *, row_number() over (partition by job_title order by salary desc, id) as rnk
    from employees
)
select
    id,
    job_title,
    salary,
    dept_id
from top_employees
where rnk <= 3
