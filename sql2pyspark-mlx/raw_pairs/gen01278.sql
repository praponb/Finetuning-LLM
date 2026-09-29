SELECT duration_sec_bucket, pct_home, total_duration_sec
FROM (
    SELECT CASE WHEN duration_sec >= 404 THEN 'high' WHEN duration_sec >= 136 THEN 'mid' ELSE 'low' END duration_sec_bucket, ROUND((100 * SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_home, SUM(duration_sec) total_duration_sec
    FROM web_events
    WHERE session_id = 4
    GROUP BY CASE WHEN duration_sec >= 404 THEN 'high' WHEN duration_sec >= 136 THEN 'mid' ELSE 'low' END
) agg
WHERE total_duration_sec >= 1100
ORDER BY total_duration_sec DESC;
