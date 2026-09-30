WITH prepared AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        ABS(debut_year - 2015) AS debut_year_distance
    FROM artists
    WHERE country IN ('BR', 'JP', 'US')
      AND is_verified = TRUE
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(debut_year_distance / 500) * 500 AS debut_year_distance_bucket,
        ROUND(debut_year_distance / NULLIF(debut_year, 0), 3) AS debut_year_distance_per_debut_year,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM prepared
    WHERE debut_year > 1997
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.duration_sec BETWEEN 164 AND 392
              AND t.title IS NOT NULL
        )
      AND s.debut_year <= 2016
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(debut_year_distance_bucket) OVER (PARTITION BY country_or_unknown ORDER BY debut_year_distance ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS debut_year_distance_bucket_avg_3,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY country_or_unknown, genre)) AS distinct_country_in_country_or_unknown_genre
    FROM with_activity
),
totals AS (
    -- Group-level totals
    SELECT
        distinct_country_in_country_or_unknown_genre,
        country_or_unknown,
        is_verified,
        ARRAY_SORT(COLLECT_SET(country)) AS country_list,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
        SUM(distinct_country_in_country_or_unknown_genre) AS total_distinct_country_in_country_or_unknown_gen,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN country IS NOT NULL THEN debut_year_distance ELSE 0 END) AS debut_year_distance_country_is_not_null
    FROM metrics
    GROUP BY distinct_country_in_country_or_unknown_genre, country_or_unknown, is_verified
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE total_distinct_country_in_country_or_unknown_gen < (SELECT MAX(total_distinct_country_in_country_or_unknown_gen) FROM totals)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_ids, country_or_unknown, total_distinct_country_in_country_or_unknown_gen
    FROM screened
) src
PIVOT (
    MIN(total_distinct_country_in_country_or_unknown_gen)
    FOR country_or_unknown IN ('TH', 'KR')
)
