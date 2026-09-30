WITH latest AS (
    SELECT *
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time NULLS FIRST, id) = 1
)
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    AVG(duration_sec) FILTER (WHERE event_type = 'page_view') AS page_view_avg_duration_sec,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    MIN(event_time) AS first_seen,
    MIN(duration_sec) AS min_duration_sec,
    GROUPING_ID() AS grouping_id
FROM latest
GROUP BY ROLLUP (DATE_TRUNC('WEEK', event_time), DATE_TRUNC('MONTH', event_time))
ORDER BY page_view_avg_duration_sec DESC NULLS LAST;
