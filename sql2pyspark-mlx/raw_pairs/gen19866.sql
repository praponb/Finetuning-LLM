with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        trim(full_name) as clean_full_name
    from employees
    where hire_date >= date '2023-02-25'
      and level is distinct from 'L1'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        length(full_name) as full_name_length,
        coalesce(job_title, 'UNKNOWN') as job_title_clean
    from filtered
    where hire_date >= date '2023-03-21'
      and manager_id is not null
),
enriched as (
    -- Ranking and gap features
    select
        id,
        full_name,
        email,
        job_title,
        full_name_length,
        last_value(full_name_length) ignore nulls over (partition by date_trunc('week', hire_date), manager_id order by hire_date, id rows between unbounded preceding and current row) as last_known_full_name_length,
        row_number() over (partition by job_title order by hire_date desc, id) as event_seq
    from cleaned
),
scored as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        email,
        job_title,
        full_name_length,
        event_seq,
        avg(last_known_full_name_length) over w_recent as running_avg_last_known_full_name_length,
        max(last_known_full_name_length) over w_recent as running_max_last_known_full_name_length,
        count(last_known_full_name_length) over w_recent as running_count_last_known_full_name_length
    from enriched
    window w_recent as (partition by job_title order by last_known_full_name_length, id rows between unbounded preceding and current row)
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    job_title,
    metric,
    reading
from (
    select
        id,
        job_title,
        running_avg_last_known_full_name_length,
        running_max_last_known_full_name_length
    from scored
) src
unpivot include nulls (
    reading for metric in (running_avg_last_known_full_name_length as RUNNING_AVG_LAST_KNOWN_FULL_NAME_LENGTH_VAL, running_max_last_known_full_name_length)
)
