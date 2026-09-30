WITH Base AS (
    SELECT *
    FROM web_events
    WHERE HOUR(event_time) BETWEEN 6 AND 13
),
summary AS (
    SELECT CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, ROUND((100 * SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_click
    FROM Base
    GROUP BY CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM summary
ORDER BY event_cohort
LIMIT 5;
