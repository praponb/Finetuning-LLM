with with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by device order by started_at asc, id asc) as prev_started_at
    from sessions
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval '90' day then 1 else 0 end as session_start_flag
    from with_prev
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device order by started_at asc, id asc rows between unbounded preceding and current row) as session_no
    from flagged
)
-- Multi-level aggregation
select
    case when session_no >= 7 then 'LARGE' else 'SMALL' end as session_no_tier,
    approx_percentile(session_start_flag, 0.5) as session_start_flag_p50,
    max(started_at) as last_seen,
    percentile_approx(session_start_flag, array(0.25, 0.5, 0.75)) as session_start_flag_quartiles
from sessions_sessions
group by case when session_no >= 7 then 'LARGE' else 'SMALL' end
