WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE YEAR(release_date) = 2024
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.moods_count,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM scoped_tracks s
    JOIN plays p ON p.track_id = s.id
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.is_skipped = TRUE
        )
      AND s.play_source = 'playlist'
),
joined_artist AS (
    -- Join artists
    SELECT
        s.*,
        a.artist_name,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified,
        p.is_skipped AS play_is_skipped_2,
        p.source AS play_source_2
    FROM screened s
    JOIN artists a ON a.id = s.artist_id
    LEFT JOIN plays p ON p.id = s.play_id
)
SELECT DISTINCT play_source, title
FROM joined_artist
ORDER BY title NULLS LAST
