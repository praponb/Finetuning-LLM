SELECT
    we.duration_sec,
    s.customer_id,
    s.browser,
    CONCAT(s.browser, '/', s.country) AS browser_country
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id;
