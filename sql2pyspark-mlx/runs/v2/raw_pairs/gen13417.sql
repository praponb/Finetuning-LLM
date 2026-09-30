with recent_employees as (
    select *
    from employees
    where hire_date >= '2025-07-01' and hire_date <= '2024-09-09'
),
employees_stats as (
    select
        level,
        date_trunc('MONTH', hire_date) as hire_month,
        max(hire_date) as last_hire_date,
        avg(salary) as avg_salary
    from recent_employees
    group by level, date_trunc('MONTH', hire_date)
)
select *
from employees_stats
where avg_salary < 109000
order by level
