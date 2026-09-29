SELECT
    page,
    DATE_TRUNC('MONTH', event_time) event_month,
    event_type,
    COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) page_view_count,
    ROUND((100 * SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_home,
    COUNT(duration_sec) duration_sec_count
FROM web_events
WHERE page IN ('/search', '/checkout') AND event_type IN ('purchase', 'add_to_cart')
GROUP BY page, DATE_TRUNC('MONTH', event_time), event_type;
