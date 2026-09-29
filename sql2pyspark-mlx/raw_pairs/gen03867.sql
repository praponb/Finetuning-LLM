with valid as (
    -- Keep recent, valid rows
    select
        *,
        cast(is_remote as int) as is_remote_flag,
        coalesce(level, 'DIRECT') as level_filled
    from employees
    where hire_date > date '2023-07-23'
      and dept_id in (28, 37)
),
joined as (
    -- Attach departments attributes
    select
        src.id,
        src.dept_id,
        src.job_title,
        src.salary,
        src.hire_date,
        src.is_remote,
        src.level,
        src.is_remote_flag,
        d.budget,
        d.location,
        d.dept_name
    from valid src
    left join departments d
        on src.dept_id = d.id
    where d.location is not null
),
scored as (
    -- Ranking and gap features
    select
        id,
        job_title,
        is_remote,
        budget,
        location,
        dept_name,
        sum(salary) over w_part as last3_sum_salary,
        count(salary) over w_part as last3_count_salary,
        max(salary) over w_part as last3_max_salary,
        avg(salary) over w_part as last3_avg_salary,
        last_value(job_title, true) over w_grp as last_known_job_title
    from joined
    window w_part as (partition by location, dept_name order by is_remote_flag, id rows between 2 preceding and current row), w_grp as (partition by level, dept_name order by is_remote_flag, id rows between unbounded preceding and current row)
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        job_title,
        is_remote,
        location,
        last3_sum_salary,
        last3_count_salary,
        last3_max_salary,
        last_known_job_title,
        cast(is_remote as int) as is_remote_flag,
        round(budget * 0.85, 2) as budget_scaled,
        coalesce(job_title, 'N/A') as resolved_job_title
    from scored
    where is_remote
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        is_remote_flag * last3_count_salary as is_remote_flag_times_last3_count_salary,
        coalesce(location, 'N/A') as resolved_location
    from cleaned
    where job_title is not null
)
-- Running and moving aggregates
select
    id,
    job_title,
    location,
    last3_sum_salary,
    last3_count_salary,
    last3_max_salary,
    budget_scaled,
    resolved_job_title,
    is_remote_flag_times_last3_count_salary,
    sum(last3_max_salary) over (partition by location order by is_remote_flag, id rows between unbounded preceding and current row) as cumulative_last3_max_salary,
    ntile(3) over (partition by location, job_title order by last3_count_salary desc, id) as last3_count_salary_bucket,
    rank() over w_running as budget_scaled_rank,
    ntile(10) over w_grp as last3_max_salary_bucket
from base
window w_running as (partition by location, job_title order by budget_scaled nulls last), w_grp as (partition by job_title order by last3_max_salary desc, id);
