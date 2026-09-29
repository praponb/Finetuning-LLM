with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        substring(full_name, 1, 3) as full_name_prefix,
        initcap(trim(job_title)) as job_title_title
    from employees
    where manager_id is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        manager_id,
        salary,
        hire_date,
        is_remote,
        job_title_title,
        lead(salary) over (partition by dept_id order by salary, id) as next_salary,
        lag(salary) over (partition by dept_id order by salary, id) as prev_salary,
        row_number() over (partition by dept_id order by salary, id) as event_seq,
        sum(salary) over (partition by level order by salary, id rows between unbounded preceding and current row) as running_salary
    from filtered
),
base as (
    -- Basic cleansing and defaults
    select *, upper(email) as email_upper
    from with_windows
    where hire_date >= date '2023-05-16'
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by manager_id order by running_salary desc, id) as rn
    from base
    qualify rn = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        full_name,
        is_remote,
        prev_salary,
        event_seq,
        running_salary,
        email_upper,
        rn
    from deduped
    qualify row_number() over (partition by manager_id order by next_salary desc nulls last, id) = 1
),
with_windows_employees as (
    -- Ranking and gap features
    select
        id,
        full_name,
        prev_salary,
        event_seq,
        running_salary,
        email_upper,
        ntile(5) over w_grp as running_salary_bucket,
        percent_rank() over (order by running_salary desc nulls first) as running_salary_percent_rank,
        last_value(running_salary) ignore nulls over (order by event_seq, id rows between unbounded preceding and current row) as last_known_running_salary
    from ranked
    window w_grp as (order by running_salary desc, id)
)
-- Running and moving aggregates
select
    id,
    prev_salary,
    event_seq,
    running_salary,
    email_upper,
    running_salary_bucket,
    last_known_running_salary,
    max(event_seq) over (order by running_salary_bucket, id rows between unbounded preceding and current row) as running_max_event_seq,
    count(event_seq) over (order by running_salary_bucket, id rows between unbounded preceding and current row) as running_count_event_seq,
    avg(event_seq) over (order by running_salary_bucket, id rows between unbounded preceding and current row) as running_avg_event_seq,
    sum(event_seq) over (order by running_salary_bucket, id rows between unbounded preceding and current row) as running_sum_event_seq,
    avg(event_seq) over (order by running_salary_percent_rank, id rows between 5 preceding and 1 preceding) as event_seq_avg_5_prior
from with_windows_employees
