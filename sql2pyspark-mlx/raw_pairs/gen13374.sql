select
    s.device,
    s.customer_id,
    s.country,
    we.event_time,
    we.session_id
from sessions s
join web_events we on s.id = we.session_id
where we.page in ('/product', '/search', '/cart')
