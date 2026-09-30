WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        ABS(debut_year - 2015) AS debut_year_distance
    FROM artists
    WHERE (NOT is_verified OR debut_year <= 2018)
)
SELECT artist_name
FROM base
EXCEPT
SELECT title AS artist_name
FROM tracks
ORDER BY artist_name DESC
