SELECT event_cohort, pct_checkout, duration_sec_count
FROM (
    SELECT CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END AS event_cohort, ROUND((100 * SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checkout, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    GROUP BY CASE WHEN YEAR(event_time) < 2024 THEN 'before_2024' WHEN YEAR(event_time) = 2024 THEN '2024' ELSE 'after_2024' END
) AS sub
WHERE pct_checkout > 45;
