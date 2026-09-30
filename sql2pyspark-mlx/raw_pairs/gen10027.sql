WITH deduped AS (
    SELECT *
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY started_at DESC NULLS LAST, id) = 1
)
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    DATE_TRUNC('YEAR', started_at) AS year_start,
    DATE_TRUNC('WEEK', started_at) AS week_start,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count,
    COUNT(DISTINCT browser) AS distinct_browsers,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    MIN(started_at) AS started_at,
    MIN(started_at) AS first_seen
FROM deduped
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', started_at), DATE_TRUNC('YEAR', started_at), DATE_TRUNC('WEEK', started_at)), (DATE_TRUNC('QUARTER', started_at), DATE_TRUNC('YEAR', started_at)), (DATE_TRUNC('QUARTER', started_at)))
ORDER BY distinct_browsers DESC;
