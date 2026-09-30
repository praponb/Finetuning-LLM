WITH latest AS (
    SELECT *
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC NULLS LAST, id) = 1
)
SELECT
    CASE WHEN duration_sec >= 389 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    COUNT(*) FILTER (WHERE page = '/checkout') AS checkout_rows,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    AVG(duration_sec) AS avg_duration_sec_2
FROM latest
GROUP BY duration_sec_tier
