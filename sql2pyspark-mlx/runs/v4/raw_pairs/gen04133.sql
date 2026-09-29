SELECT CASE WHEN duration_sec >= 440 THEN 'high' WHEN duration_sec >= 214 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
FROM web_events
WHERE TO_DATE(event_time) >= '2024-07-30' AND session_id IN (16, 17, 29, 30) AND session_id = 19
GROUP BY duration_sec_bucket
HAVING ROUND(STDDEV(duration_sec), 2) > 35
ORDER BY stddev_duration_sec, duration_sec_bucket
LIMIT 10;
