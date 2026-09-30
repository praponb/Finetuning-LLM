WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE is_explicit = FALSE
),
enriched AS (
    -- Add calculated columns
    SELECT *, ABS(duration_sec - 279) AS duration_sec_distance
    FROM prepared
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.bpm_per_duration_sec,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM enriched s
    JOIN plays p ON p.track_id = s.id
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.moods,
        s.bpm_per_duration_sec,
        s.play_id,
        s.play_played_at,
        s.play_source,
        s.play_is_skipped,
        p.id AS play_id_2,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped_2,
        p.device AS play_device
    FROM exploded_detail s
    JOIN plays p ON p.track_id = s.id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(play_device), ' ', '_')) AS play_device_slug
    FROM line_level_plays
    WHERE ARRAY_CONTAINS(moods, 'party')
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.device <> 'car' THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_device_count
    FROM scored s
)
-- Subtotals with ROLLUP
SELECT
    artist_id,
    play_id,
    SUM(CASE WHEN play_device = 'car' THEN 1 ELSE 0 END) AS n_play_device_car
FROM profiled
GROUP BY ROLLUP(artist_id, play_id)
ORDER BY artist_id NULLS LAST, play_id NULLS LAST
