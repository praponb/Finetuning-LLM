WITH scoped_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        ABS(debut_year - 2006) AS debut_year_distance,
        LOWER(TRIM(artist_name)) AS artist_name_clean
    FROM artists
    WHERE debut_year <= 2020
      AND LOWER(artist_name) LIKE '%te%'
),
latest AS (
    -- Keep the latest row per country
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        debut_year_distance,
        artist_name_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY debut_year DESC, id DESC) AS row_num
        FROM scoped_artists
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        genre,
        artist_name_clean,
        RANK() OVER w_country_by_debut_year AS debut_year_rank,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY debut_year, id) AS rn,
        SIZE(COLLECT_SET(genre) OVER (PARTITION BY country)) AS distinct_genre_in_country,
        MIN(debut_year_distance) OVER (PARTITION BY country ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_debut_year_distance_so_far,
        COUNT(*) OVER (PARTITION BY country) AS rows_in_country,
        debut_year_distance - LAG(debut_year_distance) OVER (PARTITION BY country ORDER BY debut_year, id) AS debut_year_distance_change
    FROM latest
    WINDOW w_country_by_debut_year AS (PARTITION BY country ORDER BY debut_year, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(artist_name_clean)) AS artist_name_clean_len,
        CONCAT_WS(' / ', artist_name_clean, genre) AS artist_name_clean_genre_label
    FROM with_windows
),
latest_per_key AS (
    -- Keep the latest row per genre
    SELECT
        id,
        genre,
        artist_name_clean,
        debut_year_rank,
        rn,
        distinct_genre_in_country,
        min_debut_year_distance_so_far,
        rows_in_country,
        debut_year_distance_change,
        artist_name_clean_len,
        artist_name_clean_genre_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY genre ORDER BY distinct_genre_in_country DESC, id DESC) AS row_num
        FROM tagged
    ) ranked
    WHERE row_num = 1
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        artist_name_clean,
        debut_year_rank,
        rn,
        rows_in_country,
        debut_year_distance_change,
        artist_name_clean_genre_label,
        FLOOR(rn / 2) * 2 AS rn_bucket
    FROM latest_per_key
)
SELECT
    id,
    rows_in_country,
    debut_year_distance_change,
    artist_name_clean_genre_label,
    rn_bucket,
    GREATEST(rows_in_country, rn_bucket) AS greatest_rows_in_country_rn_bucket
FROM derived
WHERE artist_name_clean IS NOT NULL
