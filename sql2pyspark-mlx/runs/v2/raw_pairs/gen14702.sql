SELECT CASE WHEN duration_sec >= 479 THEN 'high' WHEN duration_sec >= 152 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, COUNT(duration_sec) AS duration_sec_count, MAX(event_time) AS latest_event_time
FROM web_events
WHERE page != '/product'
GROUP BY CASE WHEN duration_sec >= 479 THEN 'high' WHEN duration_sec >= 152 THEN 'mid' ELSE 'low' END;
