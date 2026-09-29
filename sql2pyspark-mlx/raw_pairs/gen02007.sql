with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        if(is_remote, 'Yes', 'No') as is_remote_label,
        trim(job_title) as clean_job_title
    from employees
    where hire_date >= to_date('2023-04-02')
      and manager_id is not null
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        clean_job_title
    from valid
    qualify row_number() over (partition by manager_id order by hire_date desc nulls last, id) = 1
),
valid_employees as (
    -- Keep recent, valid rows
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        clean_job_title,
        trim(full_name) as clean_full_name
    from deduped
    where lower(trim(email)) = 'kanya@mail.co.th'
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        clean_job_title
    from (
        select *, row_number() over (partition by job_title, dept_id order by hire_date nulls last, id) as rank_in_group
        from valid_employees
    ) t
    where rank_in_group = 1
)
-- Window metrics over several different partitions
select
    id,
    full_name,
    dept_id,
    level,
    first_value(job_title) ignore nulls over (partition by dept_id order by hire_date, id rows between unbounded preceding and current row) as first_known_job_title
from latest;
