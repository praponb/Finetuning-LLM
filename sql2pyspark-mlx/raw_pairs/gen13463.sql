SELECT we.id, we.page, we.event_time, s.customer_id FROM web_events we FULL OUTER JOIN sessions s ON we.session_id = s.id WHERE we.page IS NOT NULL ORDER BY id;
