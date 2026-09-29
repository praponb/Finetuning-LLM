WITH with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.event_type
    FROM sessions s
    JOIN LATERAL (
            SELECT c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    device,
    DATE_TRUNC('WEEK', started_at) AS week_start,
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    SIZE(COLLECT_SET(device)) AS num_devices,
    COUNT(DISTINCT country) AS distinct_countries,
    SIZE(COLLECT_SET(device)) AS num_devices_2,
    GROUPING_ID() AS grouping_id
FROM with_latest
GROUP BY GROUPING SETS ((device, DATE_TRUNC('WEEK', started_at), DATE_TRUNC('MONTH', started_at)), (device, DATE_TRUNC('WEEK', started_at)), (device), ())
HAVING SIZE(COLLECT_SET(device)) > 1;
