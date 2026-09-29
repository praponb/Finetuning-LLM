SELECT
    sessions.browser,
    sessions.country,
    SUM(web_events.duration_sec) AS total_duration_sec,
    SUM(CASE WHEN web_events.event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count
FROM web_events
INNER JOIN sessions ON web_events.session_id = sessions.id
WHERE sessions.started_at >= '2023-04-12'
GROUP BY sessions.browser, sessions.country
HAVING SUM(web_events.duration_sec) > 2600;
