WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix
    FROM plays
    WHERE track_id IS NOT NULL
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source_prefix,
        prev_played_at,
        new_session_flag,
        NTH_VALUE(session_no, 2) OVER (PARTITION BY source ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_no,
        ROUND((UNIX_TIMESTAMP(prev_played_at) - UNIX_TIMESTAMP(LAG(prev_played_at) OVER w_source_by_ms_played)) / 60, 1) AS minutes_since_prev_prev_played_at,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        COUNT(*) OVER (PARTITION BY source ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM sessionized
    WINDOW w_source_by_ms_played AS (PARTITION BY source ORDER BY ms_played, id)
),
with_parent_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source_prefix,
        s.prev_played_at,
        s.new_session_flag,
        s.second_session_no,
        s.rows_in_device,
        s.running_count,
        l.username AS listener_username,
        t.title AS track_title,
        t.release_date AS track_release_date
    FROM with_windows s
    JOIN listeners l ON l.id = s.listener_id AND l.plan IN ('premium', 'family', 'free')
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    is_skipped,
    prev_played_at,
    new_session_flag,
    second_session_no,
    rows_in_device,
    running_count,
    listener_username,
    track_release_date,
    COALESCE(second_session_no, 0) AS second_session_no_filled
FROM with_parent_listener
ORDER BY second_session_no_filled
