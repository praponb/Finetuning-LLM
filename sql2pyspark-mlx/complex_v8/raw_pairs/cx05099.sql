WITH filtered_plays AS (
    -- Filter plays
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
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        track_id,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, session_no
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.track_id,
        s.session_no,
        s.session_end,
        s.event_count,
        s.duration_min,
        p.id AS play_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device
    FROM per_session s
    JOIN plays p ON p.track_id = s.track_id
)
SELECT track_id, session_no, measure, metric_value
FROM (
    SELECT track_id, session_no, play_ms_played, event_count
    FROM with_children_plays
) src
UNPIVOT (
    metric_value FOR measure IN (play_ms_played AS play_ms_played_v, event_count AS m_event_count)
) unp
