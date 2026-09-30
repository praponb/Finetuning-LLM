with web_events_by_session as (
    select session_id, min(event_time) as min_event_time
    from web_events
    group by session_id
)
select s.id, s.browser, webs.min_event_time
from sessions s
join web_events_by_session webs on s.id = webs.session_id;
