SELECT event_type
FROM web_events
EXCEPT ALL
SELECT event_type
FROM web_events
WHERE event_type != 'search'
