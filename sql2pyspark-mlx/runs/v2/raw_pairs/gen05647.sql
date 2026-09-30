select
    s.browser,
    s.customer_id,
    s.started_at,
    we.session_id,
    we.event_time
from sessions s
left outer join web_events we on s.id = we.session_id
order by s.customer_id desc;
