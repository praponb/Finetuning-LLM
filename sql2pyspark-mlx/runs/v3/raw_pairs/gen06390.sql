with valid_employees as (
    select *, coalesce(full_name, 'N/A') full_name_filled
    from employees
    where length(email) >= 8 and email like '%@gmail.com' and hire_date >= '2023-04-19'
),
summary as (
    select level, max(hire_date) latest_hire_date
    from valid_employees
    group by level
)
select level, latest_hire_date
from summary;
