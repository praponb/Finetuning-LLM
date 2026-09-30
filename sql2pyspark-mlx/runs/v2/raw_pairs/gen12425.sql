SELECT CASE WHEN duration_sec >= 450 THEN 'high' WHEN duration_sec >= 90 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, MAX(duration_sec) AS max_duration_sec FROM web_events GROUP BY 1
