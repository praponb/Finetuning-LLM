SELECT
    sessions.customer_id,
    sessions.device,
    web_events.event_type,
    web_events.duration_sec,
    web_events.event_time
FROM sessions
LEFT OUTER JOIN web_events ON web_events.session_id = sessions.id
WHERE sessions.device != 'desktop'
ORDER BY customer_id
