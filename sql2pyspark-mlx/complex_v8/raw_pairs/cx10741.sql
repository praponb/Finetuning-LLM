WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
),
with_prev AS (
    -- Previous played_at per device_or_unknown
    SELECT *, LAG(played_at) OVER (PARTITION BY device_or_unknown ORDER BY played_at, id) AS prev_played_at
    FROM base_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY device_or_unknown ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        device_or_unknown,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY device_or_unknown, session_no
)
-- Final projection
SELECT
    device_or_unknown,
    session_start,
    session_end,
    events,
    duration_min,
    COALESCE(duration_min, 0.0) AS duration_min_filled
FROM sessions
ORDER BY duration_min_filled DESC NULLS FIRST, device_or_unknown
