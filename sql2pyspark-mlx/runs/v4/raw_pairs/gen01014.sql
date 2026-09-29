WITH src AS (
    SELECT *
    FROM web_events
    WHERE page NOT IN ('/home', '/cart', '/product')
),
event_type_summary AS (
    SELECT
        event_type,
        TO_DATE(event_time) event_day,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) event_type_values,
        ROUND((100 * SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_product
    FROM src
    GROUP BY event_type, TO_DATE(event_time)
)
SELECT
    event_type,
    event_day,
    event_type_values,
    pct_product
FROM event_type_summary
