with filtered_web_events as (
    select *
    from web_events
    where event_time < '2023-06-01' and (hour(event_time) between 12 and 17 or session_id in (1, 22, 23, 32))
),
summary as (
    select
        event_type,
        page,
        count(distinct duration_sec) as unique_event_types,
        avg(duration_sec) as avg_duration_sec
    from filtered_web_events
    group by event_type, page
)
select *
from summary
where unique_event_types >= 4
