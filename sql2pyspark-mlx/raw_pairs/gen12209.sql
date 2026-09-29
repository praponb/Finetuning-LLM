with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        coalesce(email, 'N/A') as email_filled
    from employees
    where hire_date > '2024-04-01'
),
base as (
    -- Keep recent, valid rows
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        email_filled,
        concat(job_title, ' - ', full_name) as job_title_full_name
    from valid
    where is_remote = false
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        count(salary) over (partition by month(hire_date), dept_id) as hire_date_month_count_salary,
        cume_dist() over (partition by dept_id order by salary desc) as salary_cume_dist,
        row_number() over w_part as row_seq
    from base
    window w_part as (partition by dept_id order by salary, id)
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        hire_date,
        hire_date_month_count_salary,
        salary_cume_dist,
        row_seq
    from (
        select *, row_number() over (partition by job_title, level order by hire_date_month_count_salary desc, id) as rank_in_group
        from with_windows
    ) x
    where rank_in_group = 1
),
daily as (
    -- Grouped metrics
    select
        date_trunc('quarter', hire_date) as quarter_start,
        approx_percentile(salary_cume_dist, 0.9) as salary_cume_dist_p90,
        count(*) as event_count,
        round(avg(salary_cume_dist), 2) as avg_salary_cume_dist
    from latest
    group by date_trunc('quarter', hire_date)
)
select
    quarter_start,
    salary_cume_dist_p90,
    event_count,
    avg_salary_cume_dist
from daily
order by salary_cume_dist_p90;
