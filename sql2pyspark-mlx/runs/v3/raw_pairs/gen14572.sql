SELECT
    we.id,
    we.event_time,
    we.event_type,
    we.page
FROM web_events we
WHERE we.event_type NOT IN ('add_to_cart', 'click', 'search') AND we.session_id NOT IN (SELECT id FROM sessions WHERE browser IN ('Edge', 'Safari'))
