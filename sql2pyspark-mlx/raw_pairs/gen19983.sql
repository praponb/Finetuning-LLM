SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    event_type,
    SUM(CASE WHEN event_type = 'purchase' THEN duration_sec ELSE 0 END) AS purchase_duration_sec,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    AVG(duration_sec) AS avg_duration_sec
FROM web_events
GROUP BY 1, 2
HAVING STDDEV_SAMP(duration_sec) > 0
ORDER BY purchase_duration_sec DESC;
