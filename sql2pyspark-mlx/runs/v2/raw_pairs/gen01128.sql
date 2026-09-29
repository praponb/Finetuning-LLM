SELECT
    s.id,
    s.country,
    COUNT(we.id) web_event_count,
    SUM(CASE WHEN we.event_type = 'click' THEN 1 ELSE 0 END) click_count
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE we.duration_sec BETWEEN 180 AND 260
GROUP BY s.id, s.country
ORDER BY country, id
LIMIT 10;
