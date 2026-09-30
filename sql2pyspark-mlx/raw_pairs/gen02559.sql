WITH deduped AS (
    SELECT *
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) = 1
)
SELECT
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    country,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    COUNT(CASE WHEN browser = 'Chrome' THEN 1 END) AS chrome_count,
    MIN(started_at) AS first_seen,
    COUNT_IF(country = 'DE') AS de_events,
    MIN(started_at) AS started_at,
    MAX(started_at) AS last_seen,
    GROUPING(DATE_TRUNC('MONTH', started_at)) AS grouping_level
FROM deduped
GROUP BY ROLLUP (DATE_TRUNC('MONTH', started_at), country)
ORDER BY de_events
