select s.started_at, we.event_time from sessions s left join web_events we on we.session_id = s.id where we.page in ('/checkout', '/home') and s.customer_id is null
