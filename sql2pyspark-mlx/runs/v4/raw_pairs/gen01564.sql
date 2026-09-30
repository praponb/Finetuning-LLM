SELECT page, MIN(duration_sec) AS min_duration_sec
FROM web_events
WHERE session_id IN (1, 23, 40)
GROUP BY page
ORDER BY page;
