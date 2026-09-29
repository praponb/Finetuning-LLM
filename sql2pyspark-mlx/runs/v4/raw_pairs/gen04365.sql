SELECT event_cohort, max_duration_sec
FROM (
    SELECT CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    GROUP BY CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
) AS t
WHERE max_duration_sec < 130;
