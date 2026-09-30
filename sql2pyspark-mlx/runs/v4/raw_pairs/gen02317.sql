WITH base AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        TRIM(event_type) AS clean_event_type,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE event_time >= TO_DATE('2023-12-22')
)
SELECT
    event_type,
    AVG(duration_sec) AS avg_duration_sec,
    PERCENTILE_APPROX(duration_sec, 0.95) AS duration_sec_p95,
    SUM(CASE WHEN event_type = 'click' THEN duration_sec ELSE 0 END) AS click_duration_sec,
    SIZE(COLLECT_SET(event_type)) AS num_event_types
FROM base
GROUP BY event_type
