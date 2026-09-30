WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at,
        COALESCE(country, 'NONE') AS country_filled
    FROM sessions
    WHERE device IN ('desktop', 'tablet')
      AND started_at > TO_TIMESTAMP('2023-02-27 04:00:00')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    device,
    COUNT(*) FILTER (WHERE browser = 'Firefox') AS firefox_rows,
    ROUND(AVG(session_id), 2) AS avg_session_id,
    COUNT(*) FILTER (WHERE browser = 'Edge') AS edge_rows
FROM sessions_sessions
GROUP BY device
HAVING avg_session_id < 1000000
