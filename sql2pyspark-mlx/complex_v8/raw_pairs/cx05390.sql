WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
joined AS (
    -- Join tracks
    SELECT
        s.*,
        t.bpm AS track_bpm,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title
    FROM sessionized s
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    prev_played_at,
    is_new_session,
    session_id,
    track_bpm,
    track_duration_sec,
    ABS(session_id - 2) AS session_id_distance,
    DATE_TRUNC('HOUR', played_at) AS played_at_hour
FROM joined
ORDER BY session_id_distance, prev_played_at NULLS LAST
