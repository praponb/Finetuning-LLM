SELECT
    page,
    DATE_FORMAT(event_time, 'yyyy-MM') AS event_month,
    QUARTER(event_time) AS event_quarter,
    MAX(duration_sec) AS max_duration_sec,
    MIN(event_time) AS first_event_time,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
FROM web_events
WHERE event_type = 'purchase' AND event_type IN ('page_view', 'add_to_cart', 'search') AND duration_sec > 158
GROUP BY page, event_month, event_quarter
HAVING max_duration_sec < 310
