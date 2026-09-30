WITH scoped_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket,
        CASE WHEN genre IN ('Jazz', 'HipHop') THEN 'group_a' WHEN genre = 'Classical' THEN 'group_b' ELSE 'other' END AS genre_group
    FROM artists
    WHERE country IN ('TH', 'BR')
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        artist_name,
        country,
        debut_year_bucket,
        genre_group,
        CONCAT(country, '#', CAST(debut_year AS STRING)) AS country_ref,
        CONCAT(country, '#', CAST(debut_year_bucket AS STRING)) AS country_ref_2,
        CASE WHEN debut_year_bucket >= 2008 THEN 'large' WHEN debut_year_bucket >= 2003 THEN 'medium' ELSE 'small' END AS debut_year_bucket_tier
    FROM scoped_artists
)
SELECT *
FROM (
    SELECT debut_year_bucket_tier, country, genre_group, debut_year_bucket
    FROM annotated
) src
PIVOT (
    SUM(debut_year_bucket)
    FOR genre_group IN ('group_a', 'other', 'group_b')
)
ORDER BY debut_year_bucket_tier, country
