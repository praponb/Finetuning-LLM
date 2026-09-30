SELECT CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, COUNT(*) AS num_web_events, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
WHERE session_id IN (12, 26)
GROUP BY CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
