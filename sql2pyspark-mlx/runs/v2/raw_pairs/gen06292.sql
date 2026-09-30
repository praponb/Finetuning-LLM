select
    id,
    hire_date,
    email,
    salary,
    coalesce(level, 'N/A') as level_filled
from employees
where job_title not in ('Manager', 'Designer')
