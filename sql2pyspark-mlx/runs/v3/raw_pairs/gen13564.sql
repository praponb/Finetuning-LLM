SELECT
    s.started_at,
    s.browser,
    we.event_time,
    CASE WHEN we.duration_sec >= 538 THEN 'High' WHEN we.duration_sec >= 104 THEN 'Medium' ELSE 'Low' END AS duration_sec_level
FROM sessions s
LEFT OUTER JOIN web_events we ON we.session_id = s.id
WHERE s.country = 'UK' OR we.session_id IN (8, 16, 20, 28)
ORDER BY browser DESC;
