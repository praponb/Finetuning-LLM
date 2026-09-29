with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by session_id order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by session_id order by event_time, id) + interval '14' day then 1 else 0 end as is_new_session
    from web_events
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '30' day then 1 else 0 end as is_new_burst
    from islands
),
islands_web_events as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id_2
    from flagged
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        is_new_burst * burst_id_2 as is_new_burst_times_burst_id_2,
        datediff(current_date(), event_time) / 7 as weeks_since_event_time
    from islands_web_events
    where page is not null
      and event_time >= '2023-07-01'
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lag(weeks_since_event_time) over w_hist as prev_weeks_since_event_time,
        lead(weeks_since_event_time) over w_hist as next_weeks_since_event_time,
        lag(is_new_session) over w_hist as prev_is_new_session,
        lead(is_new_session) over w_hist as next_is_new_session,
        row_number() over w_hist as event_seq,
        sum(duration_sec) over (partition by page order by weeks_since_event_time, id rows between unbounded preceding and current row) as running_duration_sec
    from filtered
    where is_new_burst <> 1
    window w_hist as (partition by session_id, event_type order by burst_id_2, id)
),
with_gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '7' day then 1 else 0 end as new_group_flag
    from enriched
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from with_gaps_2
)
select
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    is_new_session,
    burst_id,
    is_new_burst,
    burst_id_2,
    case when event_type rlike '^(add_to_cart|page_view)$' then 'GROUP_B' else 'OTHER_ZONE' end as event_type_zone
from sessionized
