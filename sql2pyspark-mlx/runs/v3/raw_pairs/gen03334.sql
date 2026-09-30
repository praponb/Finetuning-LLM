SELECT
    s.device,
    DATE_TRUNC('MONTH', s.started_at) AS started_month,
    ROUND(STDDEV(we.duration_sec), 2) AS stddev_duration_sec,
    MIN(we.duration_sec) AS min_duration_sec
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
GROUP BY s.device, DATE_TRUNC('MONTH', s.started_at)
ORDER BY stddev_duration_sec
