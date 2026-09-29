SELECT
    s.customer_id,
    s.country,
    s.browser,
    we.page,
    we.duration_sec
FROM sessions s
JOIN web_events we ON s.id = we.session_id
WHERE s.customer_id IS NOT NULL
ORDER BY we.duration_sec DESC;
