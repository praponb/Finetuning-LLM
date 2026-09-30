SELECT
    page,
    SUM(duration_sec) AS total_duration_sec,
    COUNT(*) AS cnt,
    ROUND(SUM(duration_sec) * 100.0 / SUM(SUM(duration_sec)) OVER (), 2) AS pct_of_total
FROM web_events
WHERE event_time < '2025-08-03'
GROUP BY page
ORDER BY page;
