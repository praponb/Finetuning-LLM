SELECT s.id, s.customer_id, t.first_event_time FROM sessions s INNER JOIN (SELECT session_id, MIN(event_time) AS first_event_time FROM web_events GROUP BY session_id) t ON s.id = t.session_id
