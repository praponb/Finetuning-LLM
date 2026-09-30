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
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        ABS(ms_played - 221269) AS ms_played_distance
    FROM plays
    WHERE device IN ('car', 'speaker', 'tv')
      AND ms_played < 287769
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ms_played_distance,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM filtered_plays
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped ORDER BY ms_played_distance DESC), 3) AS ms_played_distance_percent_rank,
        ROUND(PERCENT_RANK() OVER w_is_skipped_by_ms_played_dist_2, 3) AS ms_played_distance_percent_rank_2,
        MIN(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far
    FROM annotated
    WINDOW w_is_skipped_by_ms_played_dist_2 AS (PARTITION BY is_skipped ORDER BY ms_played_distance DESC, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    ms_played_distance,
    is_skipped_label,
    ms_played_distance_percent_rank,
    CONCAT_WS('-', device, is_skipped_label) AS device_is_skipped_label_label,
    LENGTH(TRIM(device)) AS device_len
FROM windowed
ORDER BY id DESC, track_id
