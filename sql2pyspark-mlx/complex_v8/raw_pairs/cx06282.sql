WITH recent_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        debut_year,
        is_verified,
        CONCAT(genre, '#', CAST(debut_year AS STRING)) AS genre_ref,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        SUBSTRING(TRIM(country), 1, 3) AS country_prefix
    FROM artists
    WHERE country = 'US'
),
grouped AS (
    -- Summarise per group
    SELECT
        is_verified_flag,
        is_verified,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
        MAX(is_verified_flag) AS max_is_verified_flag,
        ROUND(SUM(is_verified_flag) / NULLIF(SUM(debut_year), 0), 3) AS is_verified_flag_to_debut_year_ratio,
        ROUND(STDDEV(debut_year), 2) AS stddev_debut_year
    FROM recent_artists
    GROUP BY 1, 2
),
derived AS (
    -- Add calculated columns
    SELECT
        is_verified_flag,
        is_verified,
        max_is_verified_flag,
        stddev_debut_year,
        SIZE(artist_name_list) AS artist_name_list_count
    FROM grouped
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE max_is_verified_flag > (SELECT AVG(max_is_verified_flag) FROM derived)
)
-- Native SQL UNPIVOT (wide to long)
SELECT is_verified_flag, is_verified, measure, amount
FROM (
    SELECT is_verified_flag, is_verified, artist_name_list_count, max_is_verified_flag
    FROM qualified
) src
UNPIVOT (
    amount FOR measure IN (artist_name_list_count, max_is_verified_flag)
) unp
ORDER BY is_verified_flag, is_verified, measure
