WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        INITCAP(TRIM(source)) AS source_clean,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        CASE WHEN device IN ('car', 'phone') THEN 'group_a' WHEN device = 'desktop' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE track_id >= 3
      AND ms_played >= 51883
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        device,
        source,
        source_clean,
        ms_played_bucket,
        device_group,
        MIN(ms_played_bucket) OVER (PARTITION BY track_id) AS track_id_min_ms_played_bucket,
        NTILE(4) OVER (PARTITION BY device_group, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played_bucket NULLS LAST, id) AS ms_played_bucket_quartile,
        ROUND(AVG(ms_played) OVER (PARTITION BY device_group, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played_bucket NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_3,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id), 2) AS ms_played_vs_track_id_avg,
        SIZE(COLLECT_SET(device_group) OVER (PARTITION BY track_id)) AS distinct_device_group_in_track_id,
        LAST_VALUE(ms_played_bucket) IGNORE NULLS OVER (PARTITION BY device_group, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played_bucket NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_bucket
    FROM valid
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        s.source_clean,
        s.ms_played_bucket,
        s.track_id_min_ms_played_bucket,
        s.ms_played_avg_3,
        s.ms_played_vs_track_id_avg,
        s.distinct_device_group_in_track_id,
        s.last_known_ms_played_bucket,
        t.release_date AS track_release_date,
        t.artist_id AS track_artist_id,
        t.is_explicit AS track_is_explicit
    FROM windowed s
    JOIN tracks t ON t.id = s.track_id
)
SELECT
    track_id,
    track_is_explicit,
    GROUPING(track_id) AS is_total_track_id,
    MAX(track_release_date) AS last_track_release_date,
    MIN(last_known_ms_played_bucket) AS min_last_known_ms_played_bucket
FROM with_parent
GROUP BY CUBE(track_id, track_is_explicit)
ORDER BY track_id NULLS LAST, track_is_explicit NULLS LAST
