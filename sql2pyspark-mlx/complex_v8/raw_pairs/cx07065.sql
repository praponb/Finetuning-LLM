WITH prepared AS (
    -- Scope the artists rows
    SELECT
        id,
        genre,
        country,
        debut_year,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE is_verified = TRUE
      AND artist_name IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(country_slug, '#', CAST(debut_year_filled AS STRING)) AS country_slug_ref,
        GREATEST(debut_year, debut_year_filled) AS greatest_debut_year_debut_year_filled,
        ABS(debut_year_filled - 2003) AS debut_year_filled_distance
    FROM prepared
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM scored
    WHERE debut_year_filled < (SELECT MAX(debut_year_filled) FROM scored)
)
SELECT
    id,
    genre,
    country,
    debut_year,
    country_slug,
    debut_year_filled,
    country_slug_ref,
    greatest_debut_year_debut_year_filled,
    debut_year_filled_distance,
    LEAST(debut_year, debut_year_filled) AS least_debut_year_debut_year_filled
FROM screened
WHERE country_slug IS NOT NULL
ORDER BY country, id
LIMIT 10
