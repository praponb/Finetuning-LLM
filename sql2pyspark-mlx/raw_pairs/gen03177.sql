SELECT we.id, we.page, we.session_id FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE customer_id IN (9, 32)) ORDER BY we.session_id DESC;
