WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        INITCAP(TRIM(artist_name)) AS artist_name_clean,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE country = 'US'
),
scored AS (
    -- Derive helper columns
    SELECT *, INITCAP(TRIM(artist_name)) AS artist_name_clean_2
    FROM cleaned
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, debut_year, debut_year_filled
    FROM scored
) src
UNPIVOT (
    metric_value FOR field IN (debut_year, debut_year_filled)
) unp
