WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE is_explicit = TRUE
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, a.genre AS artist_genre, a.debut_year AS artist_debut_year
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        bpm - artist_debut_year AS bpm_minus_artist_debut_year,
        CONCAT(artist_genre, '#', CAST(artist_debut_year AS STRING)) AS artist_genre_ref,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM with_details
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT is_explicit, artist_id, artist_genre, duration_sec_per_bpm
    FROM labeled
) src
PIVOT (
    COUNT(duration_sec_per_bpm) AS count_v, MAX(duration_sec_per_bpm) AS max_v
    FOR artist_genre IN ('Pop', 'Indie', 'HipHop')
)
ORDER BY is_explicit, artist_id
