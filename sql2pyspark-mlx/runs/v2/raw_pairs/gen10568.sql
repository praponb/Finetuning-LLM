SELECT CASE WHEN duration_sec >= 405 THEN 'high' WHEN duration_sec >= 73 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, SUM(duration_sec) AS total_duration_sec, SUM(SUM(duration_sec)) OVER (ORDER BY CASE WHEN duration_sec >= 405 THEN 'high' WHEN duration_sec >= 73 THEN 'mid' ELSE 'low' END) AS cumulative_duration_sec
FROM web_events
GROUP BY CASE WHEN duration_sec >= 405 THEN 'high' WHEN duration_sec >= 73 THEN 'mid' ELSE 'low' END
