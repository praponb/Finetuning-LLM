with recent_employees as (
    select *
    from employees
    where hire_date < '2025-05-01'
),
is_remote_summary as (
    select is_remote, max(salary) - min(salary) as salary_range
    from recent_employees
    group by is_remote
)
select is_remote, salary_range
from is_remote_summary;
