SELECT
    CASE WHEN duration_sec >= 495 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    COUNT(*) AS event_count
FROM web_events
GROUP BY CASE WHEN duration_sec >= 495 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) > 1
ORDER BY event_count ASC NULLS LAST
