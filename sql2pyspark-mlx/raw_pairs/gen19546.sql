select *
from (
    select
        level,
        dept_id,
        job_title,
        salary
    from employees
) src
pivot (
    avg(salary) as total
    for job_title in ('Analyst' as analyst, 'Engineer' as engineer, 'Designer' as designer)
);
