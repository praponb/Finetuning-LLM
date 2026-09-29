SELECT
    we.session_id,
    we.duration_sec,
    s.browser,
    s.device
FROM web_events we
LEFT OUTER JOIN sessions s ON we.session_id = s.id
WHERE TO_DATE(s.started_at) < '2025-09-04'
