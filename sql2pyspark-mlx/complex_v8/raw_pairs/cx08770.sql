WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        CASE WHEN ms_played >= 291723 THEN 'A' WHEN ms_played >= 71476 THEN 'B' ELSE 'C' END AS ms_played_tier,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE source = 'search'
      AND track_id IS NOT NULL
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device_slug,
        s.ms_played_tier,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM base_plays s
    JOIN plays p ON p.listener_id = s.listener_id AND p.is_skipped = TRUE
)
-- Final projection
SELECT
    id,
    listener_id,
    ms_played_tier,
    play_id,
    play_is_skipped,
    play_device,
    play_played_at,
    play_ms_played
FROM detail_rows
