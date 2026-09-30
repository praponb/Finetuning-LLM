SELECT we.id, we.page FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE HOUR(started_at) BETWEEN 10 AND 14) ORDER BY we.page DESC;
