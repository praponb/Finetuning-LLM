SELECT
    CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort,
    page,
    MAX(event_time) AS latest_event_time,
    SUM(CASE WHEN event_type = 'click' THEN duration_sec ELSE 0 END) AS click_duration_sec
FROM web_events
WHERE HOUR(event_time) BETWEEN 6 AND 9 AND page IS NOT NULL
GROUP BY event_cohort, page
HAVING click_duration_sec > 52;
