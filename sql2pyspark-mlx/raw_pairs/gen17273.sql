WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        browser,
        started_at,
        SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    burst_id,
    browser,
    MAX(started_at) AS last_seen,
    SUM(burst_id) AS total_burst_id
FROM islands
GROUP BY GROUPING SETS ((burst_id, browser), (burst_id), ());
