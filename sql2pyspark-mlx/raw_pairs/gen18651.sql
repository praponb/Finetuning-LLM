WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        browser,
        started_at,
        country,
        SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Grouped metrics
SELECT
    CASE WHEN island_id >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS island_id_tier,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(island_id, 0.9) AS island_id_p90,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    MIN(started_at) AS first_started_at,
    PERCENTILE_APPROX(island_id, 0.9) AS island_id_p90_2,
    MIN(started_at) AS started_at
FROM sessionized
GROUP BY 1;
