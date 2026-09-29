SELECT
    s.country,
    s.customer_id,
    we.event_type,
    INITCAP(TRIM(s.browser)) AS browser_title
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE we.duration_sec < 290 AND we.duration_sec BETWEEN 246 AND 334
ORDER BY browser_title
