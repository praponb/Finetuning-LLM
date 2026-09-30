WITH metrics AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        LAST_VALUE(browser) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_browser
    FROM sessions
)
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    device,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    COUNT(*) FILTER (WHERE device = 'tablet') AS tablet_rows,
    GROUPING_ID() AS gid
FROM metrics
GROUP BY ROLLUP (DATE_TRUNC('YEAR', started_at), DATE_TRUNC('QUARTER', started_at), device)
ORDER BY device NULLS FIRST;
