WITH filtered_web_events AS (
    SELECT *
    FROM web_events
    WHERE event_type IN ('page_view', 'purchase') AND (event_time < '2024-06-01' OR duration_sec BETWEEN 236 AND 355)
),
web_events_stats AS (
    SELECT CASE WHEN duration_sec >= 435 THEN 'high' WHEN duration_sec >= 158 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, AVG(duration_sec) AS avg_duration_sec
    FROM filtered_web_events
    GROUP BY CASE WHEN duration_sec >= 435 THEN 'high' WHEN duration_sec >= 158 THEN 'mid' ELSE 'low' END
)
SELECT duration_sec_bucket, duration_sec_range
FROM web_events_stats
ORDER BY duration_sec_bucket;
