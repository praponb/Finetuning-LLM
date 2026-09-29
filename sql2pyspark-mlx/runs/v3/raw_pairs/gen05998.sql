SELECT page, MAX(duration_sec) AS max_duration_sec
FROM web_events
GROUP BY page
ORDER BY max_duration_sec
