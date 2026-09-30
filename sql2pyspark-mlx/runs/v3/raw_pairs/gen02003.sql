select
    s.browser,
    s.started_at,
    we.event_time,
    we.duration_sec
from sessions s
left outer join web_events we on s.id = we.session_id;
