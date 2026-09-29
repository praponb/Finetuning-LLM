SELECT
    HOUR(event_time) AS event_hour,
    SUM(CASE WHEN page = '/cart' THEN duration_sec ELSE 0 END) AS cart_duration_sec,
    COUNT(duration_sec) AS duration_sec_count,
    ROUND((100 * SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_purchase
FROM web_events
WHERE event_type IN ('add_to_cart', 'page_view', 'click')
GROUP BY 1
