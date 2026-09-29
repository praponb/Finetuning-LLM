WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.duration_sec
    FROM sessionized s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_latest
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM with_gaps_2
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessions_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN session_no_2 >= 6 THEN 'LARGE' ELSE 'SMALL' END AS session_no_2_tier,
    browser,
    COUNT(*) AS num_rows,
    ROUND(SUM(session_start_flag) / NULLIF(COUNT(CASE WHEN browser = 'Edge' THEN 1 END), 0), 2) AS session_start_flag_per_edge
FROM kept
GROUP BY GROUPING SETS ((CASE WHEN session_no_2 >= 6 THEN 'LARGE' ELSE 'SMALL' END, browser), (CASE WHEN session_no_2 >= 6 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY browser
