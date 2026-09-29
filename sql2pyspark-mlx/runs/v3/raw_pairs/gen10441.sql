SELECT page, duration_sec
FROM web_events
WHERE duration_sec > 394
INTERSECT
SELECT page, duration_sec
FROM web_events
WHERE HOUR(event_time) BETWEEN 11 AND 16;
