select s.browser, we.event_time, we.page
from sessions s
left join web_events we on we.session_id = s.id
where we.page in ('/product', '/home', '/search')
