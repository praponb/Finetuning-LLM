select
    s.customer_id,
    s.started_at,
    s.country,
    we.duration_sec,
    we.event_time,
    we.session_id
from sessions s
left join web_events we on we.session_id = s.id
order by s.country desc
