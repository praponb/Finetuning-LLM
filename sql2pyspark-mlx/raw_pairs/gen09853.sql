SELECT CASE WHEN duration_sec >= 417 THEN 'high' WHEN duration_sec >= 173 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, session_id, COUNT(*) AS web_event_count
FROM web_events
WHERE session_id IN (9, 15, 32, 35) AND duration_sec IS NOT NULL AND event_type IN ('purchase', 'search', 'add_to_cart')
GROUP BY 1, 2
