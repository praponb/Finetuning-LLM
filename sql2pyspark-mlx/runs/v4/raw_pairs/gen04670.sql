with gaps as (
    select *, case when event_time - lag(event_time) over (partition by session_id order by event_time, id) > interval '30' day then 1 else 0 end as new_group_flag
    from web_events
),
sessionized as (
    select *, sum(new_group_flag) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_no
    from gaps
)
select
    date_trunc('week', event_time) as week_start,
    round(avg(duration_sec), 2) as avg_duration_sec,
    (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as span_minutes,
    percentile_approx(session_no, array(0.25, 0.5, 0.75)) as session_no_quartiles
from sessionized
group by date_trunc('week', event_time)
order by week_start nulls first
