WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tracks
),
with_activity AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM recent_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.debut_year >= 1996
              AND a.country = 'BR'
        )
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM with_activity s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.country = 'US'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(duration_sec - 344) AS duration_sec_distance,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM joined_artist
    WHERE artist_genre = 'Classical'
)
-- Final projection
SELECT
    id,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    artist_debut_year,
    duration_sec_distance,
    duration_sec_filled,
    COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown
FROM labeled
WHERE artist_debut_year < 2018
