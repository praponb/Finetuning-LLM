select
    s.device,
    s.customer_id,
    s.browser,
    we.event_time,
    we.event_type,
    round(we.duration_sec * 1.07, 2) as duration_sec_adjusted
from sessions s
left join web_events we on we.session_id = s.id;
