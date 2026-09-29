SELECT
    CASE WHEN YEAR(s.started_at) < 2024 THEN 'before_2024' WHEN YEAR(s.started_at) = 2024 THEN '2024' ELSE 'after_2024' END AS started_cohort,
    CASE WHEN s.country IN ('UK', 'SG') THEN 'core' ELSE 'other' END AS country_group,
    ROUND(STDDEV(we.duration_sec), 2) AS stddev_duration_sec,
    MIN(we.event_time) AS min_event_time
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
WHERE we.duration_sec < 360
GROUP BY CASE WHEN YEAR(s.started_at) < 2024 THEN 'before_2024' WHEN YEAR(s.started_at) = 2024 THEN '2024' ELSE 'after_2024' END, CASE WHEN s.country IN ('UK', 'SG') THEN 'core' ELSE 'other' END;
