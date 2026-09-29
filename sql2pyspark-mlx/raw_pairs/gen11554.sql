SELECT
    we.session_id,
    s.customer_id,
    s.started_at,
    s.country,
    COALESCE(we.page, 'N/A') page_filled
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
ORDER BY s.country;
