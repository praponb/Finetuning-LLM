select we.id, we.page, we.session_id from web_events we where we.session_id in (select id from sessions where started_at < '2024-05-01')
