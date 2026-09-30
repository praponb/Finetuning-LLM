with valid as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        cast(is_remote as int) is_remote_flag
    from employees
    where hire_date >= date '2023-09-30'
      and job_title is not null
      and salary >= 96000
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws('-', full_name, job_title) full_name_job_title_key,
        coalesce(job_title, 'N/A') job_title_filled
    from valid
    where hire_date > date '2023-04-12'
      and job_title is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from cleaned
    qualify row_number() over (partition by dept_id, job_title order by hire_date, id) = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        count(is_remote_flag) over (partition by dept_id) dept_id_count_is_remote_flag,
        avg(is_remote_flag) over (order by salary, id rows between 5 preceding and 1 preceding) is_remote_flag_avg_5_prior,
        first_value(job_title) ignore nulls over (partition by job_title order by hire_date, id rows between unbounded preceding and current row) first_known_job_title
    from deduped
),
with_dims as (
    -- Attach departments attributes
    select
        src.*,
        d.dept_name,
        d.budget
    from metrics src
    inner join departments d
        on src.dept_id = d.id
)
select
    id,
    full_name,
    email,
    dept_id,
    is_remote_flag,
    full_name_job_title_key,
    job_title_filled,
    first_known_job_title
from with_dims
order by is_remote_flag
