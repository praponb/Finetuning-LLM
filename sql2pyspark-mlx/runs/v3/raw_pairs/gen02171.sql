SELECT page, COUNT(duration_sec) AS duration_sec_count
FROM web_events
GROUP BY page
HAVING MIN(duration_sec) > 390
ORDER BY 2 DESC;
