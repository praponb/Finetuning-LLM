SELECT event_type, session_id
FROM web_events
WHERE event_time >= '2025-01-04'
UNION ALL
SELECT event_type, session_id
FROM web_events
WHERE event_time < '2023-10-01'
