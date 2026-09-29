WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_id >= 7 THEN 'HIGH' ELSE 'LOW' END AS session_id_tier,
    COUNT_IF(browser = 'Edge') AS edge_events,
    PERCENTILE_APPROX(session_id, ARRAY(0.25, 0.5, 0.75)) AS session_id_quartiles,
    ROUND(AVG(new_group_flag), 2) AS avg_new_group_flag,
    MIN(started_at) AS first_seen
FROM sessions_sessions
GROUP BY CASE WHEN session_id >= 7 THEN 'HIGH' ELSE 'LOW' END
HAVING avg_new_group_flag > 0
ORDER BY avg_new_group_flag
