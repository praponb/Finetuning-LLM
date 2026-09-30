WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    browser,
    MIN(started_at) AS first_seen,
    APPROX_PERCENTILE(session_id, ARRAY(0.25, 0.5, 0.75)) AS session_id_quartiles,
    COUNT_IF(browser = 'Safari') AS safari_events
FROM islands
GROUP BY CUBE (CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END, browser)
ORDER BY safari_events DESC NULLS LAST
