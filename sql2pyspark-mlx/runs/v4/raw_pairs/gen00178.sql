SELECT session_id, event_type
FROM web_events
INTERSECT ALL
SELECT session_id, event_type
FROM web_events
WHERE page <> '/checkout';
