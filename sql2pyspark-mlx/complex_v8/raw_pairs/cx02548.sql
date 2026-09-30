WITH scoped AS (
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
    WHERE played_at >= TIMESTAMP '2025-02-06 12:00:00'
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM scoped
    WHERE ms_played > (SELECT MIN(ms_played) FROM scoped)
),
grouped AS (
    -- Group-level totals
    SELECT
        track_id,
        listener_id,
        COUNT(DISTINCT device) AS distinct_devices,
        MIN(ms_played) AS min_ms_played,
        MIN(played_at) AS first_played_at
    FROM with_activity
    WHERE track_id >= 6
    GROUP BY track_id, listener_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        HOUR(first_played_at) AS first_played_at_hour,
        distinct_devices + min_ms_played AS distinct_devices_plus_min_ms_played
    FROM grouped
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.track_id,
        s.listener_id,
        s.distinct_devices,
        s.min_ms_played,
        s.first_played_at_hour,
        p.id AS play_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.track_id AS play_track_id
    FROM annotated s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        track_id,
        listener_id,
        distinct_devices,
        min_ms_played,
        first_played_at_hour,
        play_id,
        play_track_id,
        CASE WHEN min_ms_played >= 281500 THEN 'large' WHEN min_ms_played >= 143250 THEN 'medium' ELSE 'small' END AS min_ms_played_tier,
        LOWER(TRIM(play_source)) AS play_source_clean
    FROM exploded_detail
)
-- Final projection
SELECT
    track_id,
    listener_id,
    distinct_devices,
    min_ms_played,
    first_played_at_hour,
    play_id,
    play_track_id,
    min_ms_played_tier,
    SUBSTRING(TRIM(play_source_clean), 1, 4) AS play_source_clean_prefix,
    LENGTH(TRIM(min_ms_played_tier)) AS min_ms_played_tier_len
FROM enriched
ORDER BY first_played_at_hour DESC, min_ms_played_tier_len, track_id, listener_id, play_id
LIMIT 10
