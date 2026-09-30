with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        event_type,
        event_time,
        duration_sec,
        length(page) as page_length
    from web_events
    where event_time > timestamp '2024-05-06 21:00:00'
      and event_time >= '2023-12-01'
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by event_type order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by event_type order by event_time, id) + interval '14' day then 1 else 0 end as new_group_flag
    from base
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as island_id
    from flagged
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        event_type,
        event_time,
        page_length,
        case when duration_sec >= 362 then 'High' when duration_sec >= 117 then 'Medium' else 'Low' end as duration_sec_band,
        cast(new_group_flag as double) as new_group_flag_num
    from sessionized
    where event_time >= date '2023-04-23'
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by event_type order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by event_type order by event_time, id) + interval '90' day then 1 else 0 end as is_new_burst
    from valid
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as session_no
    from flagged_2
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from islands
    qualify row_number() over (partition by event_type order by event_time desc, id) = 1
)
-- Aggregate with conditional metrics
select
    event_type,
    date_trunc('quarter', event_time) as quarter_start,
    count(*) as event_count,
    count_if(event_type = 'page_view') as page_view_events
from latest
group by event_type, date_trunc('quarter', event_time)
