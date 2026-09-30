WITH prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) AS prev_started_at
    FROM sessions
),
gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_sessions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
    ROUND(AVG(session_start_flag), 2) AS avg_session_start_flag,
    APPROX_PERCENTILE(session_start_flag, 0.95) AS session_start_flag_p95,
    MAX(session_start_flag) AS max_session_start_flag,
    COUNT_IF(device = 'tablet') AS tablet_events,
    SIZE(COLLECT_SET(device)) AS num_devices
FROM sessions_sessions
GROUP BY CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END;
