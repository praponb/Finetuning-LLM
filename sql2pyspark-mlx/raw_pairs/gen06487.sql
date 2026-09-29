SELECT
    event_type,
    QUARTER(event_time) AS event_quarter,
    COUNT(DISTINCT duration_sec) AS unique_sessions,
    ROUND((100 * SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search
FROM web_events
WHERE session_id IN (21, 33, 35, 40) AND duration_sec BETWEEN 387 AND 479
GROUP BY event_type, QUARTER(event_time)
HAVING COUNT(DISTINCT duration_sec) > 4
ORDER BY pct_search DESC;
