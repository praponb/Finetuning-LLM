select
    s.started_at,
    s.device,
    s.browser,
    we.event_type,
    we.duration_sec
from sessions s
inner join web_events we on s.id = we.session_id
where s.started_at >= '2023-11-08'
order by we.event_type;
