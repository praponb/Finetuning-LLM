WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE track_id IS NOT NULL
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY is_new_session, track_id)) AS distinct_device_in_is_new_session_track_id,
        NTILE(5) OVER (PARTITION BY track_id ORDER BY prev_played_at DESC, id) AS prev_played_at_quintile,
        LAG(is_new_session, 3) OVER (PARTITION BY track_id ORDER BY prev_played_at DESC, id) AS prev3_is_new_session,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY is_new_session, track_id)) AS distinct_listener_id_in_is_new_session_track_id
    FROM sessionized
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.is_new_session,
        s.distinct_device_in_is_new_session_track_id,
        s.prev_played_at_quintile,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.device AS play_device
    FROM sequenced s
    JOIN plays p ON p.listener_id = s.listener_id AND p.track_id >= 13
)
SELECT
    is_new_session,
    distinct_device_in_is_new_session_track_id,
    COUNT(DISTINCT play_track_id) AS distinct_play_track_ids
FROM detail_rows
GROUP BY ROLLUP(is_new_session, distinct_device_in_is_new_session_track_id)
ORDER BY is_new_session NULLS LAST, distinct_device_in_is_new_session_track_id NULLS LAST
