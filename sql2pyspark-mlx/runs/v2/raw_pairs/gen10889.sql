SELECT
    s.id,
    s.country,
    s.started_at,
    s.device
FROM sessions s
WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
