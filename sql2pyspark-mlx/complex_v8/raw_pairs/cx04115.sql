WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        CASE WHEN device IN ('phone', 'car') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        listener_id,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_no
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM session_summary s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played >= 56848
              AND p.track_id IS NOT NULL
        )
)
-- Final projection
SELECT
    listener_id,
    session_start,
    session_end,
    n_events,
    ABS(session_ms_played - 882250) AS session_ms_played_distance,
    CASE WHEN n_events >= 5 THEN 'large' WHEN n_events >= 3 THEN 'medium' ELSE 'small' END AS n_events_tier
FROM with_activity
