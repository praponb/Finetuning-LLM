select
    s.browser,
    s.started_at,
    we.event_type,
    we.event_time,
    we.page
from sessions s
left join web_events we on we.session_id = s.id
where we.page not in ('/cart', '/product', '/checkout')
order by event_time;
