WITH latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time
    FROM web_events
    QUALIFY RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC) = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    MAX(event_time) AS ended_at,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE event_type = 'click') AS click_rows
FROM latest
GROUP BY ROLLUP (DATE_TRUNC('WEEK', event_time))
ORDER BY distinct_ids
