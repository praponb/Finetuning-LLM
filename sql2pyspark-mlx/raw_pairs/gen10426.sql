SELECT we.session_id, we.duration_sec, s.device
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
WHERE s.country IN ('US', 'UK');
