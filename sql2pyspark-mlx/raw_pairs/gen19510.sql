SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT_IF(browser = 'Firefox') AS firefox_events,
    MIN(started_at) AS first_seen,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    SIZE(COLLECT_SET(country)) AS num_countries,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    MAX(started_at) AS last_started_at
FROM sessions
GROUP BY 1
ORDER BY num_countries DESC;
