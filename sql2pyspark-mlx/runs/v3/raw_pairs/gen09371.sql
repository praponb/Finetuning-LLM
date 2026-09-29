select
    page,
    event_type,
    duration_sec_count,
    unique_sessions
from (
    select
        page,
        event_type,
        count(duration_sec) duration_sec_count,
        count(distinct duration_sec) unique_sessions
    from web_events
    group by page, event_type
) t
where unique_sessions >= 8
order by page desc
