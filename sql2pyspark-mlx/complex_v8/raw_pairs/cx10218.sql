WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        played_at + INTERVAL 2 HOURS AS played_at_plus_2h
    FROM plays
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM recent_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.played_at_plus_2h,
        s.new_session_flag,
        s.session_no,
        t.duration_sec AS track_duration_sec,
        t.release_date AS track_release_date
    FROM sessionized s
    LEFT JOIN tracks t ON t.id = s.track_id
)
-- Final projection
SELECT
    id,
    track_id,
    is_skipped,
    played_at_plus_2h,
    new_session_flag,
    track_duration_sec,
    track_release_date,
    new_session_flag - session_no AS new_session_flag_minus_session_no
FROM with_details
ORDER BY track_id
