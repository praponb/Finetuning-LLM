SELECT
    we.id,
    we.page,
    we.event_type,
    s.country,
    s.device,
    ROUND(we.duration_sec * 0.85, 2) AS duration_sec_adjusted
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
