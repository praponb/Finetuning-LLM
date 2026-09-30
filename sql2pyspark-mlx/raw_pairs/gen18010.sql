WITH cleaned AS (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        SUBSTRING(event_type, 1, 3) AS event_type_prefix,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE duration_sec > 217
)
SELECT
    CASE WHEN duration_sec >= 439 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    MIN(event_time) AS started_at,
    PERCENTILE_APPROX(duration_sec, 0.95) AS duration_sec_p95,
    COUNT(*) AS cnt
FROM cleaned
GROUP BY CASE WHEN duration_sec >= 439 THEN 'PREMIUM' ELSE 'STANDARD' END
