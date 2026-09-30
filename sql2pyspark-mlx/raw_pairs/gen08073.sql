with enriched as (
    select
        id,
        session_id,
        duration_sec,
        lag(duration_sec) over w_running as prev_duration_sec,
        lead(duration_sec) over w_running as next_duration_sec,
        row_number() over w_running as row_seq,
        lead(duration_sec, 1) over w_grp as next_duration_sec_2
    from web_events
    window w_running as (partition by page, session_id order by duration_sec asc, id asc), w_grp as (partition by year(event_time), session_id order by duration_sec asc, id asc)
),
ranked as (
    select *
    from enriched
    qualify row_number() over (partition by session_id order by next_duration_sec desc nulls last, id asc) = 1
),
metrics as (
    select
        *,
        lead(next_duration_sec_2, 1) over (partition by session_id order by row_seq asc, id asc) as next_next_duration_sec_2,
        percent_rank() over (order by prev_duration_sec desc) as prev_duration_sec_percent_rank,
        sum(prev_duration_sec) over (partition by session_id order by duration_sec asc, id asc rows between unbounded preceding and current row) as running_prev_duration_sec
    from ranked
    where next_duration_sec < 245
),
deduped as (
    select *, row_number() over (partition by session_id order by running_prev_duration_sec desc, id asc) as rn
    from metrics
    qualify rn = 1
),
deduped_web_events as (
    select
        id,
        duration_sec,
        prev_duration_sec,
        row_seq,
        next_duration_sec_2,
        next_next_duration_sec_2,
        prev_duration_sec_percent_rank,
        running_prev_duration_sec
    from (
        select *, row_number() over (partition by session_id order by next_duration_sec asc nulls first, id asc) as rn_2
        from deduped
    ) x
    where rn_2 = 1
)
select
    id,
    prev_duration_sec,
    row_seq,
    next_next_duration_sec_2,
    prev_duration_sec_percent_rank,
    running_prev_duration_sec
from deduped_web_events
order by id desc nulls last, prev_duration_sec_percent_rank asc;
