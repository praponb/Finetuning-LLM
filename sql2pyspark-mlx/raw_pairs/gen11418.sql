SELECT
    CASE WHEN duration_sec >= 410 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/checkout' THEN 1 END), 0), 2) AS duration_sec_per_checkout,
    COUNT(*) AS num_rows,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    COUNT(DISTINCT page) AS distinct_pages
FROM web_events
GROUP BY CASE WHEN duration_sec >= 410 THEN 'PREMIUM' ELSE 'STANDARD' END;
