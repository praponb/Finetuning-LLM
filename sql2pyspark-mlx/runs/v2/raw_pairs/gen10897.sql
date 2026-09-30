SELECT page, COUNT(*) AS cnt
FROM web_events
WHERE TO_DATE(event_time) >= '2025-04-01' AND event_type IN ('purchase', 'page_view', 'add_to_cart') AND page IN ('/checkout', '/cart', '/search')
GROUP BY page;
