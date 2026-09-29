SELECT CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, MIN(event_time) AS first_event_time, MAX(duration_sec) AS max_duration_sec
FROM web_events
WHERE duration_sec = 231
GROUP BY 1
HAVING SUM(duration_sec) < 1800;
