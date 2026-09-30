SELECT we.id, we.event_time, we.event_type, s.country, c.age, UPPER(c.email) AS email_upper FROM web_events we LEFT JOIN sessions s ON s.id = we.session_id JOIN customers c ON s.customer_id = c.id;
