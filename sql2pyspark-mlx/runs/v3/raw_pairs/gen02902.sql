SELECT
    CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort,
    page,
    SUM(duration_sec) AS total_duration_sec,
    SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) AS home_count
FROM web_events
GROUP BY 1, 2
ORDER BY page, event_cohort
LIMIT 5;
