SELECT
    DATE_TRUNC('MONTH', event_time) AS event_month,
    SUM(duration_sec) AS total_duration_sec,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS duration_sec_rank
FROM web_events
GROUP BY DATE_TRUNC('MONTH', event_time);
