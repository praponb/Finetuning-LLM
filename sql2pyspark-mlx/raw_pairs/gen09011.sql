with valid as (
    -- Basic cleansing and defaults
    select
        *,
        last_day(hire_date) as hire_date_month_end,
        coalesce(level, 'N/A') as level_clean
    from employees
    where manager_id is not null
      and hire_date > to_date('2023-06-16')
)
-- Running and moving aggregates
select
    *,
    lag(salary, 1) over (partition by level order by hire_date_month_end, id) as prev_salary,
    ntile(4) over (partition by job_title order by salary desc, id) as salary_bucket
from valid;
