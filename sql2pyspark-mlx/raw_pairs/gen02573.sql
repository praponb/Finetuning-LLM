SELECT we.id, we.duration_sec
FROM web_events we
WHERE we.event_time >= '2023-04-01' AND we.session_id NOT IN (SELECT id FROM sessions WHERE device IN ('desktop', 'mobile', 'tablet'));
