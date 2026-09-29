SELECT id, session_id
FROM web_events
WHERE duration_sec BETWEEN 237 AND 369
INTERSECT
SELECT id, session_id
FROM web_events
WHERE session_id IN (11, 20, 24, 31)
