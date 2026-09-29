SELECT we.id, we.session_id FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE customer_id = 19) ORDER BY session_id DESC
