WITH recent AS (
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
        LENGTH(TRIM(device)) AS device_len,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        QUARTER(played_at) AS played_at_quarter
    FROM plays
    WHERE ms_played > 176302
      AND track_id IS NOT NULL
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.played_at_quarter,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source
    FROM recent s
    LEFT JOIN plays p ON p.track_id = s.track_id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        play_id,
        play_device,
        UPPER(TRIM(play_source)) AS play_source_clean,
        CONCAT_WS('|', play_device, play_source) AS play_device_play_source_label
    FROM line_level
    WHERE played_at_quarter >= 1
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    play_id,
    play_source_clean,
    play_device_play_source_label
FROM scored
ORDER BY play_device_play_source_label, id
