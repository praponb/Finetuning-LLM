SELECT id, page
FROM web_events
INTERSECT
SELECT id, page
FROM web_events
WHERE duration_sec > 233;
