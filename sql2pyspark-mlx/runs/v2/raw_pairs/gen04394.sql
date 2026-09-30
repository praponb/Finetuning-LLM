SELECT
    we.id,
    we.event_type,
    we.session_id,
    s.started_at,
    UPPER(s.browser) AS browser_upper
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
WHERE s.device = 'desktop' AND s.device = 'desktop'
ORDER BY browser_upper;
