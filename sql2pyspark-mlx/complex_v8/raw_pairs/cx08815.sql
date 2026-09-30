WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE listener_id >= 6
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        listener_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, burst_id
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.listener_id,
        s.burst_id,
        s.session_start,
        s.session_ms_played,
        s.duration_min,
        p.id AS play_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.track_id AS play_track_id,
        p.played_at AS play_played_at
    FROM session_summary s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
detail_rows AS (
    -- One row per play
    SELECT
        s.listener_id,
        s.burst_id,
        s.session_start,
        s.session_ms_played,
        s.duration_min,
        s.play_id,
        s.play_source,
        s.play_track_id,
        p.id AS play_id_2,
        p.ms_played AS play_ms_played_2,
        p.played_at AS play_played_at_2
    FROM with_children_plays s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    listener_id,
    burst_id,
    session_ms_played,
    duration_min,
    play_source,
    play_track_id,
    play_id_2,
    play_ms_played_2,
    play_played_at_2,
    COALESCE(play_source, 'UNKNOWN') AS play_source_or_unknown,
    LENGTH(TRIM(play_source)) AS play_source_len
FROM detail_rows
ORDER BY play_ms_played_2 DESC NULLS FIRST
