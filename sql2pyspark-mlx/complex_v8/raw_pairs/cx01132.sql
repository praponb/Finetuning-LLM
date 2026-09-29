WITH prepared AS (
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
        ABS(ms_played - 195566) AS ms_played_distance,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE ms_played <= 245877
      AND source <> 'playlist'
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.ms_played_distance,
        s.source_slug,
        p.id AS play_id,
        p.device AS play_device,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played
    FROM prepared s
    LEFT JOIN plays p ON p.track_id = s.track_id AND p.ms_played > 73625
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM exploded_detail_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.listener_id <= 17
        )
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM with_activity
    WHERE play_ms_played > (SELECT MIN(play_ms_played) FROM with_activity)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        play_id,
        MAX(ms_played) AS max_ms_played,
        SUM(play_ms_played) AS total_play_ms_played,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played,
        ARRAY_SORT(COLLECT_SET(play_device)) AS play_device_list,
        COUNT(*) AS row_count
    FROM qualified
    GROUP BY 1
    HAVING MAX(play_ms_played) > 42041
),
joined_play AS (
    -- Enrich with dimension data
    SELECT
        s.play_id,
        s.max_ms_played,
        s.total_play_ms_played,
        s.median_ms_played,
        s.play_device_list,
        s.row_count,
        p.track_id AS play_track_id,
        p.listener_id AS play_listener_id
    FROM grouped s
    LEFT JOIN plays p ON p.id = s.play_id
)
-- Final projection
SELECT
    play_id,
    total_play_ms_played,
    median_ms_played,
    play_device_list,
    row_count,
    play_track_id,
    play_listener_id,
    ABS(row_count - 5) AS row_count_distance
FROM joined_play
