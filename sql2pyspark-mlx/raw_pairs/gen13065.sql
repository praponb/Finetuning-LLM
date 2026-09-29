SELECT page, COUNT(DISTINCT duration_sec) AS unique_event_types
FROM web_events
WHERE event_type NOT IN ('search', 'click') AND duration_sec <= 139
GROUP BY page;
