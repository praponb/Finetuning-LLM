SELECT
    s.id,
    s.browser,
    COUNT(we.id) AS web_event_count,
    SUM(CASE WHEN we.page = '/cart' THEN 1 ELSE 0 END) AS cart_count
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE s.country IN ('JP', 'US')
GROUP BY s.id, s.browser
