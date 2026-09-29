WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE is_explicit = FALSE
      AND ARRAY_CONTAINS(moods, 'chill')
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped
    FROM cleaned s
    JOIN plays p ON p.track_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_source, play_is_skipped, play_device, bpm
    FROM exploded_detail_plays
) src
PIVOT (
    MAX(bpm) AS max_v, SUM(bpm) AS sum_v
    FOR play_device IN ('car', 'phone', 'tv', 'speaker')
)
ORDER BY play_source, play_is_skipped
