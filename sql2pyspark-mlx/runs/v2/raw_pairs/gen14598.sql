SELECT page, AVG(duration_sec) AS avg_duration_sec, ROUND((100 * SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checkout
FROM web_events
WHERE duration_sec BETWEEN 262 AND 374
GROUP BY page
HAVING ROUND((100 * SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 20;
