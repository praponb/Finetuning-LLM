SELECT CASE WHEN duration_sec >= 515 THEN 'high' WHEN duration_sec >= 180 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, event_type, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
GROUP BY 1, 2;
