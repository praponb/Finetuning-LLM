SELECT
    s.started_at,
    s.country,
    s.browser,
    we.event_type,
    we.page,
    we.duration_sec,
    DATEDIFF(CURRENT_DATE(), we.event_time) / 365 AS years_since_event_time
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE we.page <> '/search'
ORDER BY years_since_event_time DESC
