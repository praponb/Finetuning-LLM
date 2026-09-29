WITH scoped_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        is_verified,
        UPPER(TRIM(genre)) AS genre_clean
    FROM artists
    WHERE country = 'KR'
      AND debut_year >= 1994
)
SELECT country, id AS entity_id
FROM scoped_artists
UNION
SELECT genre AS country, id AS entity_id
FROM artists
WHERE country = 'BR'
  AND debut_year > 1997
ORDER BY country DESC
