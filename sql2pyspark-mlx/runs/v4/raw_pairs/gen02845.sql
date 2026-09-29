SELECT s.started_at, s.customer_id, we.page, DATE_FORMAT(we.event_time, 'MMM yyyy') AS event_time_label FROM sessions s JOIN web_events we ON s.id = we.session_id WHERE we.session_id = 33
