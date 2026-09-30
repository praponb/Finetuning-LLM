WITH scoped_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        genre,
        country,
        debut_year,
        INITCAP(TRIM(artist_name)) AS artist_name_clean
    FROM artists
    WHERE genre IS NOT NULL
)
SELECT id, country
FROM scoped_artists
INTERSECT ALL
SELECT artist_id AS id, title AS country
FROM tracks
WHERE is_explicit = TRUE
