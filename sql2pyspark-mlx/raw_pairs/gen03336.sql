with scored as (
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        lead(duration_sec) over (partition by session_id order by duration_sec, id) as next_duration_sec,
        lag(duration_sec) over (partition by session_id order by duration_sec, id) as prev_duration_sec,
        row_number() over (partition by session_id order by duration_sec, id) as row_seq
    from web_events
)
select
    case when duration_sec >= 516 then 'HIGH' else 'LOW' end as duration_sec_tier,
    date_trunc('month', event_time) as activity_month,
    round(avg(row_seq), 2) as avg_row_seq,
    count(case when event_type = 'click' then 1 end) as click_count,
    grouping(case when duration_sec >= 516 then 'HIGH' else 'LOW' end) as grouping_level
from scored
group by cube (case when duration_sec >= 516 then 'HIGH' else 'LOW' end, date_trunc('month', event_time))
having avg_row_seq < 1000000
