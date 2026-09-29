SELECT CASE WHEN duration_sec >= 524 THEN 'high' WHEN duration_sec >= 211 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, MIN(event_time) AS min_event_time
FROM web_events
GROUP BY 1
ORDER BY min_event_time, duration_sec_bucket;
