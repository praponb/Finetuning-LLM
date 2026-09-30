SELECT
    we.id,
    we.session_id,
    s.browser,
    s.country,
    s.started_at
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
WHERE s.device NOT IN ('tablet', 'desktop');
