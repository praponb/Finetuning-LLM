WITH base AS (
    SELECT *
    FROM web_events
    WHERE NOT (page IN ('/search', '/checkout') AND duration_sec >= 124)
),
web_events_stats AS (
    SELECT
        YEAR(event_time) AS event_year,
        CASE WHEN duration_sec >= 448 THEN 'high' WHEN duration_sec >= 61 THEN 'mid' ELSE 'low' END AS duration_sec_bucket,
        COUNT(duration_sec) AS duration_sec_count,
        MAX(event_time) AS last_event_time
    FROM base
    GROUP BY YEAR(event_time), CASE WHEN duration_sec >= 448 THEN 'high' WHEN duration_sec >= 61 THEN 'mid' ELSE 'low' END
)
SELECT
    event_year,
    duration_sec_bucket,
    duration_sec_count,
    last_event_time
FROM web_events_stats
WHERE duration_sec_count >= 7
ORDER BY duration_sec_bucket
