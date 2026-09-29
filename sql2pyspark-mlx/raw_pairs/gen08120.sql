with enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(duration_sec) over (partition by page order by event_time asc, id asc rows between unbounded preceding and current row) running_duration_sec,
        max(duration_sec) over (partition by session_id) session_id_max_duration_sec,
        avg(duration_sec) over (partition by event_type order by duration_sec asc, id asc rows between 5 preceding and current row) duration_sec_avg_5,
        lead(duration_sec, 1, 0) over (partition by session_id order by event_time asc, id asc) next_duration_sec
    from web_events
),
scored as (
    -- Running and moving aggregates
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec_avg_5,
        next_duration_sec,
        lead(duration_sec_avg_5) over (partition by event_type order by running_duration_sec asc, id asc) next_duration_sec_avg_5,
        lag(running_duration_sec) over (partition by event_type order by running_duration_sec asc, id asc) prev_running_duration_sec,
        row_number() over (partition by event_type order by running_duration_sec asc, id asc) row_seq,
        max(session_id_max_duration_sec) over (partition by session_id) session_id_max_session_id_max_duration_sec
    from enriched
    where next_duration_sec <= 395
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec_avg_5,
        next_duration_sec,
        prev_running_duration_sec,
        row_seq,
        session_id_max_session_id_max_duration_sec,
        round(prev_running_duration_sec, 1) prev_running_duration_sec_rounded
    from scored
    where session_id_max_session_id_max_duration_sec between 126 and 441
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        session_id,
        event_type,
        duration_sec_avg_5,
        next_duration_sec,
        prev_running_duration_sec,
        session_id_max_session_id_max_duration_sec,
        prev_running_duration_sec_rounded,
        coalesce(prev_running_duration_sec_rounded, 0) prev_running_duration_sec_rounded_filled
    from cleaned
    where event_time > timestamp '2023-07-11 12:00:00'
),
scored_web_events as (
    -- Ranking and gap features
    select
        id,
        session_id,
        prev_running_duration_sec_rounded,
        prev_running_duration_sec_rounded_filled,
        rank() over (partition by event_type order by next_duration_sec asc nulls last) next_duration_sec_rank,
        row_number() over w_part seq_no,
        lead(session_id_max_session_id_max_duration_sec, 2) over (partition by session_id order by prev_running_duration_sec_rounded asc, id asc) next_session_id_max_session_id_max_duration_sec,
        sum(prev_running_duration_sec_rounded) over (partition by session_id order by prev_running_duration_sec asc, id asc rows between unbounded preceding and current row) running_prev_running_duration_sec_rounded,
        min(prev_running_duration_sec_rounded_filled) over (partition by session_id) session_id_min_prev_running_duration_sec_rounded_filled
    from valid
    window w_part as (partition by session_id order by prev_running_duration_sec_rounded_filled asc, id asc)
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from scored_web_events
    qualify row_number() over (partition by session_id order by prev_running_duration_sec_rounded_filled desc nulls last, id asc) = 1
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        cast(prev_running_duration_sec_rounded_filled as int) prev_running_duration_sec_rounded_filled_int,
        next_session_id_max_session_id_max_duration_sec / nullif(session_id_min_prev_running_duration_sec_rounded_filled, 0) next_session_id_max_session_id_max_duration_sec_per_session_id_min_prev_running_duration_sec_rounded_filled
    from latest
    where next_duration_sec_rank = 6
),
totals as (
    -- Grouped metrics
    select
        case when prev_running_duration_sec_rounded_filled_int >= 76 then 'LARGE' else 'SMALL' end as prev_running_duration_sec_rounded_filled_int_tier,
        percentile_approx(prev_running_duration_sec_rounded_filled_int, array(0.25, 0.5, 0.75)) as prev_running_duration_sec_rounded_filled_int_quartiles,
        count(distinct id) as distinct_ids
    from base
    group by case when prev_running_duration_sec_rounded_filled_int >= 76 then 'LARGE' else 'SMALL' end
)
select prev_running_duration_sec_rounded_filled_int_tier, prev_running_duration_sec_rounded_filled_int_quartiles
from totals
order by prev_running_duration_sec_rounded_filled_int_tier desc
