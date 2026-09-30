SELECT
    sessions.browser,
    sessions.device,
    MAX(web_events.duration_sec) max_duration_sec,
    COUNT(CASE WHEN web_events.event_type = 'click' THEN 1 END) click_count
FROM web_events
LEFT JOIN sessions ON web_events.session_id = sessions.id
GROUP BY sessions.browser, sessions.device
HAVING max_duration_sec > 330
