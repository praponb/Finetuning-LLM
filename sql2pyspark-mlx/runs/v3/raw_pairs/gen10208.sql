SELECT MONTH(event_time) AS event_month, page, ROUND((100 * SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_page_view
FROM web_events
WHERE page IN ('/cart', '/product', '/home') AND duration_sec = 234
GROUP BY MONTH(event_time), page;
