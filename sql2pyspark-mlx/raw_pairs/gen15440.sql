WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        session_start_flag,
        burst_id,
        SUM(is_new_session) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        session_start_flag,
        burst_id,
        burst_id_2,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY session_start_flag DESC NULLS FIRST) AS session_start_flag_cume_dist,
        SUM(burst_id) OVER (PARTITION BY DATE_TRUNC('week', started_at), country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS burst_id_sum_30d
    FROM islands
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_windows
),
sessionized_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        started_at,
        country,
        burst_id,
        burst_id_2,
        session_start_flag_cume_dist,
        SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_2
)
-- Join a dimension and aggregate
SELECT
    dim.country AS customer_country,
    a.country AS session_country,
    COUNT(*) AS rows_total,
    ROUND(SUM(a.island_id), 2) AS total_island_id,
    MIN(a.burst_id_2) AS min_burst_id_2
FROM sessionized_sessions a
LEFT JOIN customers dim ON a.customer_id = dim.id
GROUP BY ROLLUP (dim.country, a.country)
