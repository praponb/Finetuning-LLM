WITH cleaned_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        debut_year,
        is_verified,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug
    FROM artists
    WHERE genre = 'HipHop'
      AND is_verified = TRUE
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN debut_year >= 2015 THEN 'HIGH' WHEN debut_year >= 1997 THEN 'MEDIUM' ELSE 'LOW' END AS debut_year_tier
    FROM cleaned_artists
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        is_verified,
        MAX(debut_year) AS max_debut_year,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
        SUM(CASE WHEN is_verified = FALSE THEN 1 ELSE 0 END) AS n_is_verified_false,
        ROUND(STDDEV(debut_year), 2) AS stddev_debut_year
    FROM tagged
    GROUP BY is_verified
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE cnt > (SELECT AVG(cnt) FROM stats)
),
scored AS (
    -- Label and score rows
    SELECT
        is_verified,
        cnt,
        artist_name_list,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        FLOOR(max_debut_year / 5) * 5 AS max_debut_year_bucket
    FROM screened
    WHERE stddev_debut_year BETWEEN 101.2 AND 1517.2
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE max_debut_year_bucket > (SELECT AVG(max_debut_year_bucket) FROM scored)
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM screened_2
    WHERE is_verified_flag > (SELECT MIN(is_verified_flag) FROM screened_2)
)
SELECT
    is_verified,
    cnt,
    artist_name_list,
    max_debut_year_bucket,
    ABS(max_debut_year_bucket - 2000) AS max_debut_year_bucket_distance,
    CASE WHEN is_verified_flag >= 1 THEN 'large' WHEN is_verified_flag >= 0 THEN 'medium' ELSE 'small' END AS is_verified_flag_tier
FROM qualified
WHERE cnt < 9
ORDER BY cnt NULLS LAST
