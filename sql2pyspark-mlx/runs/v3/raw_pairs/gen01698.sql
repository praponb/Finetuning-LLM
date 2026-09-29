SELECT page, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, MIN(duration_sec) AS min_duration_sec
FROM web_events
GROUP BY 1
HAVING ROUND(STDDEV(duration_sec), 2) < 35
ORDER BY page DESC
LIMIT 50
