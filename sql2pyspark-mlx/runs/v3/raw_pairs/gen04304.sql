with valid_employees as (
    select *
    from employees
    where not is_remote
),
agg as (
    select job_title, case when year(hire_date) < 2024 then 'before_2024' when year(hire_date) = 2024 then '2024' else 'after_2024' end as hire_cohort, max(salary) - min(salary) as salary_range
    from valid_employees
    group by job_title, case when year(hire_date) < 2024 then 'before_2024' when year(hire_date) = 2024 then '2024' else 'after_2024' end
)
select *
from agg
order by salary_range desc, hire_cohort asc
