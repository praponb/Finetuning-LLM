SELECT
    s.id,
    s.browser,
    s.device,
    s.country
FROM sessions s
WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec < 465)
ORDER BY id, device DESC
