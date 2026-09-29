SELECT event_time, page
FROM web_events
WHERE event_type IN ('search', 'click') AND event_type IN ('add_to_cart', 'page_view', 'purchase');
