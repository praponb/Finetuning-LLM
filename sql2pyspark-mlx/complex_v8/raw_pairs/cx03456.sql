WITH scoped AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug
    FROM artists
    WHERE (genre = 'Pop' OR is_verified = TRUE)
)
SELECT country_slug
FROM scoped
WHERE debut_year <= 2020
  AND is_verified = FALSE
EXCEPT
SELECT title
FROM tracks
WHERE duration_sec >= 213
ORDER BY country_slug DESC
