WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit
    FROM tracks
    WHERE duration_sec >= 213
      AND release_date BETWEEN DATE '2024-05-12' AND DATE '2025-08-28'
),
joined_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM recent_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT *
FROM (
    SELECT is_explicit, artist_genre, artist_debut_year
    FROM joined_artist
) src
PIVOT (
    COUNT(artist_debut_year)
    FOR artist_genre IN ('Jazz', 'Classical')
)
