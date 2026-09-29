with web_events_by_session as (
    select session_id, max(event_time) as latest_event_time
    from web_events
    group by session_id
)
select s.id, s.device, webs.latest_event_time
from sessions s
join web_events_by_session webs on s.id = webs.session_id
order by device, webs.latest_event_time
