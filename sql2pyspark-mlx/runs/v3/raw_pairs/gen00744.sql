select we.id, we.event_time, we.page, s.customer_id, s.country from web_events we join sessions s on s.id = we.session_id where we.duration_sec >= 219;
