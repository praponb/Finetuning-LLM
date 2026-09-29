SELECT
    event_month,
    event_cohort,
    avg_duration_sec,
    last_event_time
FROM (
    SELECT
        MONTH(event_time) AS event_month,
        CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
        MAX(event_time) AS last_event_time
    FROM web_events
    WHERE duration_sec BETWEEN 173 AND 258
    GROUP BY MONTH(event_time), CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
) AS t
WHERE avg_duration_sec < 43
ORDER BY event_month DESC;
