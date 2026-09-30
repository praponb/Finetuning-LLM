SELECT id, event_type, duration_sec
FROM web_events
WHERE duration_sec BETWEEN 155 AND 276
INTERSECT
SELECT id, event_type, duration_sec
FROM web_events
WHERE duration_sec != 312;
