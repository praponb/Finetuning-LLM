SELECT
    CASE WHEN duration_sec >= 364 THEN 'high' WHEN duration_sec >= 67 THEN 'mid' ELSE 'low' END AS duration_sec_bucket,
    SUM(duration_sec) AS total_duration_sec,
    COUNT(*) AS cnt,
    SUM(SUM(duration_sec)) OVER (ORDER BY CASE WHEN duration_sec >= 364 THEN 'high' WHEN duration_sec >= 67 THEN 'mid' ELSE 'low' END) AS cumulative_duration_sec
FROM web_events
WHERE page = '/home'
GROUP BY CASE WHEN duration_sec >= 364 THEN 'high' WHEN duration_sec >= 67 THEN 'mid' ELSE 'low' END
