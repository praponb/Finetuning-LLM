SELECT CASE WHEN YEAR(sessions.started_at) < 2024 THEN 'before_2024' WHEN YEAR(sessions.started_at) = 2024 THEN '2024' ELSE 'after_2024' END AS started_cohort, MAX(web_events.duration_sec) - MIN(web_events.duration_sec) AS duration_sec_range, COUNT(CASE WHEN web_events.event_type = 'purchase' THEN 1 END) AS purchase_count
FROM web_events
INNER JOIN sessions ON sessions.id = web_events.session_id
GROUP BY CASE WHEN YEAR(sessions.started_at) < 2024 THEN 'before_2024' WHEN YEAR(sessions.started_at) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY purchase_count DESC;
