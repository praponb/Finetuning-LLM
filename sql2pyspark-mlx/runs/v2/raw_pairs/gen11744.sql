SELECT id, duration_sec, COALESCE(page, 'N/A') page_filled
FROM web_events
WHERE event_type IN ('page_view', 'click', 'add_to_cart');
