with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        is_remote,
        level,
        if(is_remote, 'Yes', 'No') is_remote_label
    from employees
    where hire_date > to_date('2023-06-06')
      and job_title is not null
      and hire_date <= '2024-01-29'
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify rank() over (partition by manager_id order by salary asc) = 1
),
with_dims as (
    -- Attach employees attributes
    select
        src.*,
        p.email as employee_email,
        p.job_title
    from deduped src
    left join employees p
        on src.manager_id = p.id
)
-- Ranking and gap features
select
    *,
    avg(salary) over (partition by dept_id order by salary asc, id asc rows between 5 preceding and current row) salary_avg_5,
    row_number() over (partition by level order by salary asc, id asc) row_seq,
    stddev_samp(salary) over (partition by manager_id) manager_id_stddev_samp_salary
from with_dims
