WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', page, event_type) AS page_event_type_key,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE event_time >= DATE '2023-06-23'
      AND page IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec >= 363 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE event_type = 'search') AS search_rows,
    APPROX_PERCENTILE(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles
FROM base
GROUP BY CASE WHEN duration_sec >= 363 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', event_time)
ORDER BY event_count DESC;
