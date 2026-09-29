with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        initcap(trim(email)) as email_title,
        cast(is_remote as int) as is_remote_flag,
        coalesce(job_title, 'NONE') as job_title_filled
    from employees
    where hire_date > date '2023-09-03'
      and manager_id is not null
),
enriched as (
    -- Attach employees attributes
    select
        a.*,
        b.level as employee_level,
        b.full_name as employee_full_name,
        b.hire_date as employee_hire_date
    from valid a
    left join employees b
        on a.manager_id <=> b.id
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        employee_full_name,
        employee_hire_date
    from (
        select *, rank() over (partition by dept_id order by hire_date desc) as rn
        from enriched
    ) ranked
    where rn = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        job_title,
        hire_date,
        is_remote,
        employee_full_name,
        row_number() over (partition by manager_id, job_title order by employee_hire_date desc, id) as row_seq,
        first_value(job_title, true) over (partition by manager_id order by employee_hire_date, id rows between unbounded preceding and current row) as first_known_job_title,
        row_number() over w_part as event_seq,
        last_value(job_title) ignore nulls over (partition by job_title order by employee_hire_date, id rows between unbounded preceding and current row) as last_known_job_title,
        first_value(job_title) ignore nulls over w_recent as first_known_job_title_2
    from latest
    where is_remote
    window w_part as (order by hire_date, id), w_recent as (partition by manager_id order by hire_date, id rows between unbounded preceding and current row)
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        row_seq,
        first_known_job_title_2,
        max(event_seq) over w_hist as running_max_event_seq,
        count(event_seq) over w_hist as running_count_event_seq,
        avg(event_seq) over w_hist as running_avg_event_seq,
        sum(event_seq) over w_hist as running_sum_event_seq
    from with_windows
    window w_hist as (partition by job_title order by hire_date, id rows between unbounded preceding and current row)
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        row_seq,
        running_max_event_seq,
        running_count_event_seq,
        running_avg_event_seq,
        running_sum_event_seq,
        floor(running_avg_event_seq) as running_avg_event_seq_whole
    from metrics
    where row_seq = 6
),
with_windows_employees as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (order by row_seq) as row_seq_dense_rank,
        row_number() over (order by running_count_event_seq, id) as seq_no,
        lead(running_count_event_seq, 1, 0) over (order by running_avg_event_seq, id) as next_running_count_event_seq
    from filtered
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        running_avg_event_seq,
        running_max_event_seq,
        running_sum_event_seq
    from with_windows_employees
) src
unpivot include nulls (
    metric_value for metric in (running_avg_event_seq, running_max_event_seq, running_sum_event_seq)
) u
