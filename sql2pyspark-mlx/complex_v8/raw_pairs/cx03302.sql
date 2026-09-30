WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        INITCAP(TRIM(device)) AS device_clean,
        ABS(ms_played - 166517) AS ms_played_distance
    FROM plays
    WHERE device = 'speaker'
      AND ms_played <= 301149
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        is_skipped,
        listener_id,
        device_or_unknown,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        SUM(ms_played_distance) AS total_ms_played_distance,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        COUNT(*) FILTER (WHERE device NOT IN ('speaker', 'car')) AS n_device_not_in_speaker_car
    FROM cleaned
    GROUP BY is_skipped, listener_id, device_or_unknown
    HAVING AVG(ms_played) > 51883
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS min_play_ms_played,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count
    FROM totals s
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.track_id IS NOT NULL
              AND p.is_skipped = TRUE
        )
)
-- Subtotals with ROLLUP
SELECT
    play_count,
    distinct_track_ids,
    is_skipped,
    ROUND(AVG(min_play_ms_played), 2) AS avg_min_play_ms_played,
    ROUND(AVG(n_device_not_in_speaker_car), 2) AS avg_n_device_not_in_speaker_car,
    COUNT(*) FILTER (WHERE is_skipped = TRUE) AS n_is_skipped_true
FROM qualified
GROUP BY ROLLUP(play_count, distinct_track_ids, is_skipped)
