WITH cleaned AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag
    FROM artists
    WHERE UPPER(TRIM(artist_name)) LIKE 'N%'
),
stats AS (
    -- Summarise per group
    SELECT
        country,
        is_verified_flag,
        MAX(debut_year) AS max_debut_year,
        SUM(debut_year) AS total_debut_year,
        ARRAY_SORT(COLLECT_SET(genre)) AS genre_list,
        COUNT(*) FILTER (WHERE genre IS NOT NULL) AS n_genre_is_not_null
    FROM cleaned
    GROUP BY 1, 2
)
SELECT country, is_verified_flag, metric_name, reading
FROM (
    SELECT country, is_verified_flag, max_debut_year, n_genre_is_not_null
    FROM stats
) src
UNPIVOT (
    reading FOR metric_name IN (max_debut_year, n_genre_is_not_null)
) unp
ORDER BY country, is_verified_flag, metric_name
