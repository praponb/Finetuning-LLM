WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ABS(ms_played - 223828) AS ms_played_distance
    FROM plays
    WHERE listener_id IS NOT NULL
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.ms_played_distance,
        s.prev_played_at,
        s.new_session_flag,
        s.session_id,
        p.id AS play_id,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id,
        p.source AS play_source
    FROM sessionized s
    JOIN plays p ON p.track_id = s.track_id AND MONTH(p.played_at) IN (1, 3)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    source,
    ms_played_distance,
    prev_played_at,
    new_session_flag,
    session_id,
    play_played_at,
    play_listener_id,
    DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
FROM detail_rows_plays
