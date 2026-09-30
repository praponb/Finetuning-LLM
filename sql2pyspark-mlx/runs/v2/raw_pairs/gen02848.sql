SELECT
    we.id,
    we.session_id,
    we.page,
    we.event_type
FROM web_events we
WHERE we.event_type IN ('purchase', 'page_view', 'add_to_cart') AND we.session_id IN (SELECT id FROM sessions WHERE customer_id IN (5, 11, 25, 32));
