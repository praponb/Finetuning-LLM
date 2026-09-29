with with_gaps as (
    select *, case when lag(event_time) over (partition by session_id order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by session_id order by event_time, id) + interval '14' day then 1 else 0 end as new_group_flag
    from web_events
),
sessions_web_events as (
    select *, sum(new_group_flag) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
select
    case when session_no >= 6 then 'LARGE' else 'SMALL' end as session_no_tier,
    date_trunc('week', event_time) as week_start,
    round(avg(new_group_flag), 2) as avg_new_group_flag,
    approx_percentile(duration_sec, 0.5) as duration_sec_p50
from sessions_web_events
group by case when session_no >= 6 then 'LARGE' else 'SMALL' end, date_trunc('week', event_time)
order by duration_sec_p50;
