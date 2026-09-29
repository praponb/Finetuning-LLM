WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    ROUND(AVG(session_start_flag), 2) AS avg_session_start_flag,
    MAX(session_start_flag) FILTER (WHERE device = 'tablet') AS tablet_max_session_start_flag,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list
FROM sessions_sessions
GROUP BY CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END
HAVING ROUND(AVG(session_start_flag), 2) >= 0;
