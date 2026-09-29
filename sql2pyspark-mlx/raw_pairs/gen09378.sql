SELECT page
FROM web_events
EXCEPT
SELECT page
FROM web_events
WHERE event_type NOT IN ('page_view', 'click', 'search');
