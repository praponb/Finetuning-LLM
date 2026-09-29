SELECT we.id, we.duration_sec FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE device IN ('tablet', 'mobile', 'desktop'));
