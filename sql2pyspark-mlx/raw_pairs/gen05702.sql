WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', prev_started_at) AS week_start,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    MIN(started_at) AS first_seen,
    APPROX_PERCENTILE(is_new_session, 0.95) AS is_new_session_p95
FROM islands
GROUP BY DATE_TRUNC('WEEK', prev_started_at), DATE_TRUNC('QUARTER', started_at)
HAVING APPROX_PERCENTILE(is_new_session, 0.95) < 1000000;
