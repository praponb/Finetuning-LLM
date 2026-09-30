select we.id, we.session_id, we.event_time, we.page, s.customer_id, trim(s.browser) as clean_browser from web_events we inner join sessions s on we.session_id = s.id
