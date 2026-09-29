select we.id, we.duration_sec, we.event_time, we.session_id from web_events we where we.session_id not in (select id from sessions where customer_id is null)
