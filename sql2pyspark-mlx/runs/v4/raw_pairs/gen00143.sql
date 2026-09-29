SELECT
    s.id,
    s.country,
    s.device,
    s.browser
FROM sessions s
WHERE s.country IN ('US', 'TH', 'DE') AND s.id IN (SELECT session_id FROM web_events WHERE TO_DATE(event_time) >= '2025-01-01')
