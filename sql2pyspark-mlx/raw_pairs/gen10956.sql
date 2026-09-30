WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    device,
    island_id,
    ROUND(AVG(island_id), 2) AS avg_island_id,
    MIN(island_id) AS min_island_id,
    GROUPING_ID() AS grouping_level
FROM islands
GROUP BY GROUPING SETS ((device, island_id), (device))
HAVING avg_island_id < 1000000
ORDER BY device DESC NULLS FIRST;
