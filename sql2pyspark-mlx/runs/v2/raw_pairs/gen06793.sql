SELECT TO_DATE(sessions.started_at) AS started_day, SUM(CASE WHEN web_events.event_type = 'click' THEN 1 ELSE 0 END) AS click_count, ROUND(AVG(web_events.duration_sec), 2) AS avg_duration_sec
FROM web_events
LEFT JOIN sessions ON web_events.session_id = sessions.id
WHERE web_events.event_type NOT IN ('page_view', 'click')
GROUP BY TO_DATE(sessions.started_at)
