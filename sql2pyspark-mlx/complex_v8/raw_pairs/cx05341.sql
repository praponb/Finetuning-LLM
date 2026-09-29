WITH prepared_plays AS (
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
    WHERE listener_id IS NOT NULL
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MAX(played_at) AS last_played_at,
        MAX(played_at) AS last_played_at_2,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        p.last_played_at_2,
        COALESCE(p.play_count, 0) AS play_count
    FROM prepared_plays s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        played_at,
        is_skipped,
        source,
        last_played_at_2,
        play_count,
        ROUND(play_count - AVG(play_count) OVER (PARTITION BY listener_id, device), 2) AS play_count_vs_listener_id_device_avg,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id, device)) AS distinct_track_id_in_listener_id_device
    FROM with_counts
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM ranked
    WHERE distinct_track_id_in_listener_id_device > (SELECT MIN(distinct_track_id_in_listener_id_device) FROM ranked)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, distinct_track_id_in_listener_id_device, play_count
    FROM filtered
) src
UNPIVOT (
    metric_value FOR metric_name IN (distinct_track_id_in_listener_id_device, play_count)
) unp
ORDER BY id, metric_name
