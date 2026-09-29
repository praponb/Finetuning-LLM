WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
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
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        source_or_unknown,
        new_session_flag,
        MIN(session_no) OVER (PARTITION BY source, device) AS source_device_min_session_no,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY source, device)) AS distinct_track_id_in_source_device,
        DENSE_RANK() OVER (PARTITION BY source, device ORDER BY ms_played DESC, id) AS ms_played_dense_rank,
        NTH_VALUE(session_no, 2) OVER (PARTITION BY source, device ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_no,
        DENSE_RANK() OVER (ORDER BY new_session_flag DESC) AS new_session_flag_dense_rank
    FROM sessionized
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    source_or_unknown,
    new_session_flag,
    source_device_min_session_no,
    distinct_track_id_in_source_device,
    ms_played_dense_rank,
    second_session_no,
    new_session_flag_dense_rank,
    DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
FROM ranked
ORDER BY device DESC NULLS FIRST, distinct_track_id_in_source_device DESC
