WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS row_seq
    FROM web_events
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    COUNT(*) FILTER (WHERE page = '/checkout') AS checkout_rows,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    SUM(CASE WHEN event_type = 'page_view' THEN duration_sec ELSE 0 END) AS page_view_duration_sec,
    COUNT(*) AS event_count
FROM enriched
GROUP BY 1;
