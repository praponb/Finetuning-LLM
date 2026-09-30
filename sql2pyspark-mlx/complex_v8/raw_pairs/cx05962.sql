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
        source,
        CONCAT(device, '#', CAST(track_id AS STRING)) AS device_ref
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 07:00:00'
      AND listener_id IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        LAG(played_at) OVER (PARTITION BY device ORDER BY ms_played NULLS LAST, id) AS prev_played_at,
        SUM(ms_played) OVER (PARTITION BY track_id, listener_id) AS track_id_listener_id_sum_ms_played,
        MAX(ms_played) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY is_skipped, listener_id), 0), 4) AS ms_played_share_of_is_skipped_listener_id,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id, listener_id)) AS distinct_source_in_track_id_listener_id,
        COUNT(*) OVER (PARTITION BY is_skipped, listener_id) AS rows_in_is_skipped_listener_id,
        LEAD(ms_played) OVER (PARTITION BY source, track_id ORDER BY played_at, id) AS next_ms_played,
        LAG(played_at, 3) OVER (PARTITION BY listener_id, source ORDER BY ms_played DESC, id) AS prev3_played_at,
        DENSE_RANK() OVER w_source_track_id_by_played_at_2 AS played_at_dense_rank,
        RANK() OVER w_listener_id_source_by_ms_pla AS ms_played_rank,
        ROUND(PERCENT_RANK() OVER w_listener_id_device_by_ms_pla, 3) AS ms_played_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device ORDER BY played_at, id), 3) AS played_at_percent_rank
    FROM filtered_plays
    WINDOW w_source_track_id_by_played_at_2 AS (PARTITION BY source, track_id ORDER BY played_at), w_listener_id_source_by_ms_pla AS (PARTITION BY listener_id, source ORDER BY ms_played DESC, id), w_listener_id_device_by_ms_pla AS (PARTITION BY listener_id, device ORDER BY ms_played ASC NULLS LAST, id), w_is_skipped_listener_id AS (PARTITION BY is_skipped, listener_id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE ms_played_rank = 1
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    source,
    prev_played_at,
    track_id_listener_id_sum_ms_played,
    max_ms_played_so_far,
    ms_played_share_of_is_skipped_listener_id,
    distinct_source_in_track_id_listener_id,
    rows_in_is_skipped_listener_id,
    next_ms_played,
    prev3_played_at,
    ms_played_rank,
    played_at_percent_rank
FROM leaders
ORDER BY prev3_played_at DESC NULLS FIRST
