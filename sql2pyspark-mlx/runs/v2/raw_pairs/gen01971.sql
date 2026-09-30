SELECT page, SORT_ARRAY(COLLECT_SET(event_type)) event_type_list
FROM web_events
WHERE (duration_sec < 347 AND session_id IN (12, 16, 17)) OR event_type = 'search'
GROUP BY page
HAVING SUM(duration_sec) >= 2600
ORDER BY page
LIMIT 20
