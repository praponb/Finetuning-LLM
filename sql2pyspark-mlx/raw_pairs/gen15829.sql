SELECT
    CASE WHEN duration_sec >= 449 THEN 'high' WHEN duration_sec >= 119 THEN 'mid' ELSE 'low' END duration_sec_bucket,
    SUM(duration_sec) total_duration_sec,
    COUNT(*) cnt,
    SUM(SUM(duration_sec)) OVER (ORDER BY CASE WHEN duration_sec >= 449 THEN 'high' WHEN duration_sec >= 119 THEN 'mid' ELSE 'low' END) AS cumulative_duration_sec
FROM web_events
GROUP BY CASE WHEN duration_sec >= 449 THEN 'high' WHEN duration_sec >= 119 THEN 'mid' ELSE 'low' END
