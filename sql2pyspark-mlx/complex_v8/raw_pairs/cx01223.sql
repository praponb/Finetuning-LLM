WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE ms_played <= 221269
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        listener_id,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_no
),
detail_rows AS (
    -- One row per play
    SELECT
        s.listener_id,
        s.session_no,
        s.session_start,
        s.event_count,
        s.duration_min,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.ms_played AS play_ms_played
    FROM session_summary s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    listener_id,
    play_id,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(play_ms_played), 2) AS stddev_play_ms_played,
    ROUND(STDDEV(event_count), 2) AS stddev_event_count
FROM detail_rows
GROUP BY GROUPING SETS ((listener_id, play_id), (listener_id), (play_id), ())
ORDER BY listener_id NULLS LAST, play_id NULLS LAST
