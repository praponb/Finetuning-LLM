WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        LOWER(TRIM(title)) AS title_clean,
        ARRAY_CONTAINS(moods, 'focus') AS has_focus
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 6
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.id) AS avg_play_ms_played
    FROM cleaned s
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.title_clean,
        s.has_focus,
        s.play_count,
        s.avg_play_ms_played,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM with_counts s
    JOIN plays p ON p.track_id = s.id AND p.device NOT IN ('speaker', 'tv')
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.play_count,
        s.play_id,
        s.play_is_skipped,
        s.play_device,
        s.play_listener_id,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        p.ms_played AS play_ms_played
    FROM with_children s
    JOIN artists a ON a.id = s.artist_id
    LEFT JOIN plays p ON p.id = s.play_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, measure, value
FROM (
    SELECT id, play_id, play_count, play_ms_played
    FROM combined_details_artist
) src
UNPIVOT (
    value FOR measure IN (play_count, play_ms_played)
) unp
ORDER BY id, play_id, measure
