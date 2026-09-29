WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE device IN ('desktop', 'tv', 'speaker')
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        device,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY device, session_id
)
-- Final projection
SELECT
    device,
    session_start,
    session_end,
    duration_min,
    LENGTH(TRIM(device)) AS device_len,
    COALESCE(duration_min, 0.0) AS duration_min_filled
FROM per_session
ORDER BY duration_min_filled DESC, device_len
