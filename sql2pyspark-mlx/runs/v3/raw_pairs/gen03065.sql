SELECT
    we.id,
    we.page,
    we.event_time,
    s.device,
    s.customer_id,
    s.country,
    UPPER(s.country) AS country_upper
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id;
