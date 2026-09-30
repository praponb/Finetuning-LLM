SELECT page, event_cohort, avg_duration_sec
FROM (
    SELECT page, CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    GROUP BY page, CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
) AS x
WHERE avg_duration_sec > 180
ORDER BY page DESC, event_cohort
LIMIT 10;
