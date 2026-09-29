WITH cleaned_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        is_verified,
        CONCAT(country, '#', CAST(debut_year AS STRING)) AS country_ref,
        CONCAT(genre, '#', CAST(id AS STRING)) AS genre_ref,
        CONCAT_WS(' / ', country, genre) AS country_genre_label
    FROM artists
    WHERE debut_year BETWEEN 1990 AND 2006
      AND genre = 'Indie'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        country_ref,
        genre_ref,
        country_genre_label,
        CONCAT_WS('|', country, artist_name) AS country_artist_name_label,
        CASE WHEN debut_year >= 2015 THEN 'large' WHEN debut_year >= 1997 THEN 'medium' ELSE 'small' END AS debut_year_tier,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM cleaned_artists
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE debut_year > (SELECT AVG(debut_year) FROM scored)
),
derived AS (
    -- Derive helper columns
    SELECT *, COALESCE(debut_year, 0) AS debut_year_filled
    FROM with_activity
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(debut_year_tier, 'ALL') AS debut_year_tier,
    COALESCE(country, 'ALL') AS country,
    COALESCE(country_or_unknown, 'ALL') AS country_or_unknown,
    COUNT(DISTINCT country) AS distinct_countrys
FROM derived
GROUP BY GROUPING SETS ((debut_year_tier, country, country_or_unknown), (debut_year_tier))
ORDER BY debut_year_tier NULLS LAST, country NULLS LAST, country_or_unknown NULLS LAST
