WITH recent_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown
    FROM artists
    WHERE debut_year BETWEEN 1996 AND 2016
)
-- Final projection
SELECT
    id,
    genre,
    country,
    genre_or_unknown
FROM recent_artists
ORDER BY id DESC
