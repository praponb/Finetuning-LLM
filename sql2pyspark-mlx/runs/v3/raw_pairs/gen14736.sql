select
    we.id,
    we.event_type,
    we.session_id,
    s.browser,
    c.signup_date,
    c.is_active,
    round(we.duration_sec, 1) as duration_sec_rounded
from web_events we
left join sessions s on s.id = we.session_id
left join customers c on c.id = s.customer_id
where s.device = 'desktop'
