WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        CASE WHEN duration_sec >= 439 THEN 'High' WHEN duration_sec >= 106 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time
    FROM web_events
    WHERE event_time < '2025-10-27'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(page, TRUE) OVER (PARTITION BY page ORDER BY days_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page,
        ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY days_since_event_time DESC, id) AS seq_no,
        SUM(days_since_event_time) OVER (PARTITION BY session_id ORDER BY days_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_event_time
    FROM cleaned
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        d.device,
        d.browser
    FROM metrics s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        b.device AS session_device,
        b.country,
        b.browser AS session_browser
    FROM joined src
    LEFT JOIN sessions b
        ON src.session_id = b.id
)
-- Multi-level aggregation
SELECT
    country,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
    PERCENTILE_APPROX(days_since_event_time, ARRAY(0.25, 0.5, 0.75)) AS days_since_event_time_quartiles,
    AVG(days_since_event_time) FILTER (WHERE event_type = 'page_view') AS page_view_avg_days_since_event_time,
    COUNT(*) AS event_count,
    MAX(days_since_event_time) AS max_days_since_event_time,
    SIZE(COLLECT_SET(session_browser)) AS num_session_browsers
FROM with_dims
GROUP BY 1;
