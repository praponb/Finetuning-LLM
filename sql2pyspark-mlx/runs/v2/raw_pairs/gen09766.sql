SELECT
    sessions.device,
    COUNT(web_events.id) AS web_event_count,
    ROUND(STDDEV(web_events.duration_sec), 2) AS stddev_duration_sec,
    COUNT(CASE WHEN web_events.event_type = 'click' THEN 1 END) AS click_count
FROM sessions
LEFT OUTER JOIN web_events ON web_events.session_id = sessions.id
WHERE sessions.country IN ('US', 'DE', 'TH')
GROUP BY sessions.device
ORDER BY click_count DESC;
