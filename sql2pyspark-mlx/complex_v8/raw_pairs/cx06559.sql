WITH base_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CASE WHEN genre IN ('HipHop', 'Jazz') THEN 'group_a' WHEN genre = 'Pop' THEN 'group_b' ELSE 'other' END AS genre_group,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket,
        COALESCE(debut_year, 0) AS debut_year_filled
    FROM artists
    WHERE UPPER(TRIM(artist_name)) LIKE 'S%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        country,
        debut_year,
        is_verified,
        debut_year_filled,
        ROUND(debut_year / NULLIF(SUM(debut_year) OVER (PARTITION BY country, is_verified), 0), 4) AS debut_year_share_of_country_is_verified,
        SUM(debut_year_bucket) OVER (PARTITION BY is_verified, genre ORDER BY debut_year_filled DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS debut_year_bucket_sum_3,
        DENSE_RANK() OVER (PARTITION BY is_verified, genre ORDER BY debut_year_filled DESC) AS debut_year_filled_dense_rank
    FROM base_artists
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(debut_year_bucket_sum_3, debut_year) AS least_debut_year_bucket_sum_3_debut_year
    FROM with_windows
)
SELECT
    id,
    debut_year,
    debut_year_filled,
    debut_year_filled_dense_rank,
    least_debut_year_bucket_sum_3_debut_year
FROM enriched
ORDER BY debut_year NULLS LAST, id DESC
