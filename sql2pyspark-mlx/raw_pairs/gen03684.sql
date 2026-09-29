SELECT event_type AS label
FROM web_events
WHERE event_type IS NOT NULL
INTERSECT
SELECT carrier AS label
FROM shipments
WHERE carrier IS NOT NULL
