with cleaned as (
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        date_trunc('week', hire_date) as hire_date_period,
        concat(full_name, ' ', level) as full_name_level,
        coalesce(level, 'NONE') as level_clean
    from employees
    where hire_date >= to_date('2023-09-08')
      and job_title is not null
)
select
    s.id,
    s.full_name,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level_clean,
    e.email as employee_email,
    e.hire_date as employee_hire_date
from cleaned s
left join employees e
    on s.manager_id = e.id
where e.is_remote = false;
