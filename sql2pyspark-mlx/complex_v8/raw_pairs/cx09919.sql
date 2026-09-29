WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified
    FROM artists
    WHERE debut_year > 1994
)
SELECT country AS name, artist_name
FROM recent
UNION ALL
SELECT artist_name, country
FROM artists
WHERE country IN ('BR', 'JP')
ORDER BY artist_name DESC, name
