SELECT
    we.event_time,
    we.page,
    s.device,
    SUBSTRING(s.country, 1, 4) AS country_prefix
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
WHERE we.session_id IN (8, 39)
ORDER BY page
