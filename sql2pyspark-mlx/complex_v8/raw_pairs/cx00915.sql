WITH cleaned AS (
    -- Filter artists
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified,
        LOWER(TRIM(country)) AS country_clean,
        CONCAT_WS('-', genre, artist_name) AS genre_artist_name_label,
        ABS(debut_year - 1997) AS debut_year_distance
    FROM artists
    WHERE genre = 'Classical'
),
latest_per_key AS (
    -- Keep the latest row per genre
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified,
        country_clean,
        genre_artist_name_label,
        debut_year_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY genre ORDER BY debut_year_distance DESC, id DESC) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    is_verified,
    genre,
    country,
    ARRAY_SORT(COLLECT_SET(country)) AS country_list
FROM latest_per_key
GROUP BY GROUPING SETS ((is_verified, genre, country), (is_verified), ())
ORDER BY is_verified NULLS LAST, genre NULLS LAST, country NULLS LAST
