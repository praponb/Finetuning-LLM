with valid_web_events as (
    select *
    from web_events
    where session_id in (22, 24, 28) and session_id in (27, 30, 38) and session_id in (6, 9)
),
summary as (
    select event_type, concat_ws(', ', sort_array(collect_set(page))) as page_values, min(duration_sec) as min_duration_sec
    from valid_web_events
    group by event_type
)
select event_type, page_values, min_duration_sec
from summary
where min_duration_sec > 420
order by min_duration_sec desc
