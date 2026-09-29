SELECT we.id, we.event_type FROM web_events we WHERE we.page = '/home' AND we.session_id NOT IN (SELECT id FROM sessions WHERE country = 'UK') ORDER BY 1 DESC LIMIT 5;
