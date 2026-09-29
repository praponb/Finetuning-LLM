WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    country,
    COUNT_IF(browser = 'Safari') AS safari_events,
    MIN(prev_started_at) AS started_at
FROM sessions_sessions
GROUP BY country;
