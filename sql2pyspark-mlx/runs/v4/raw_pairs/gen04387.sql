SELECT
    CASE WHEN page IN ('/search', '/product') THEN 'tier_1' ELSE 'other' END page_group,
    DATE_TRUNC('MONTH', event_time) event_month,
    SORT_ARRAY(COLLECT_SET(event_type)) event_type_list,
    MAX(event_time) latest_event_time
FROM web_events
WHERE event_type != 'page_view' AND page IN ('/cart', '/checkout', '/product')
GROUP BY CASE WHEN page IN ('/search', '/product') THEN 'tier_1' ELSE 'other' END, DATE_TRUNC('MONTH', event_time)
ORDER BY page_group DESC
