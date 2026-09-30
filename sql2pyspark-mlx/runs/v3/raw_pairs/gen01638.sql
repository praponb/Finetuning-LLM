select
    s.browser,
    s.country,
    we.duration_sec,
    hour(we.event_time) as event_time_hour
from sessions s
left outer join web_events we on s.id = we.session_id
where we.event_time >= '2025-10-14';
