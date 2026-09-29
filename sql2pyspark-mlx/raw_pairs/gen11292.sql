WITH kept_keys AS (
    -- Set difference on keys
    SELECT session_id
    FROM web_events
    EXCEPT
    SELECT session_id
    FROM web_events
    WHERE event_time < '2023-11-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    INNER JOIN kept_keys k ON s.session_id = k.session_id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count,
    AVG(duration_sec) FILTER (WHERE event_type = 'click') AS click_avg_duration_sec,
    SIZE(COLLECT_SET(page)) AS num_pages
FROM kept_rows
GROUP BY DATE_TRUNC('YEAR', event_time), DATE_TRUNC('QUARTER', event_time)
ORDER BY product_count DESC;
