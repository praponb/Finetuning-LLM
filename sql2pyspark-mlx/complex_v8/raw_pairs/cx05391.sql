WITH scoped AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        LENGTH(TRIM(country)) AS country_len,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown
    FROM artists
    WHERE (artist_name IS NOT NULL OR is_verified = TRUE)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        country,
        debut_year,
        is_verified,
        genre_or_unknown,
        MAX(debut_year) OVER (PARTITION BY is_verified, genre) AS is_verified_genre_max_debut_year,
        ROUND(country_len / NULLIF(SUM(country_len) OVER (PARTITION BY country), 0), 4) AS country_len_share_of_country,
        SIZE(COLLECT_SET(genre_or_unknown) OVER (PARTITION BY country)) AS distinct_genre_or_unknown_in_country,
        ROUND(PERCENT_RANK() OVER (PARTITION BY genre, genre_or_unknown ORDER BY debut_year, id), 3) AS debut_year_percent_rank,
        SIZE(COLLECT_SET(country) OVER w_is_verified_genre) AS distinct_country_in_is_verified_genre,
        FIRST_VALUE(country_len) IGNORE NULLS OVER (PARTITION BY country, genre_or_unknown ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country_len,
        ROUND(country_len - AVG(country_len) OVER w_is_verified_genre, 2) AS country_len_vs_is_verified_genre_avg,
        NTILE(3) OVER (PARTITION BY country, genre_or_unknown ORDER BY debut_year, id) AS debut_year_tercile,
        FIRST_VALUE(debut_year) IGNORE NULLS OVER (ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_debut_year,
        SIZE(COLLECT_SET(genre) OVER w_country) AS distinct_genre_in_country,
        SUM(debut_year) OVER (ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_debut_year,
        SIZE(COLLECT_SET(genre_or_unknown) OVER (PARTITION BY is_verified, genre)) AS distinct_genre_or_unknown_in_is_verified_genre,
        ROW_NUMBER() OVER w_country_genre_or_unknown_by_2 AS seq_no,
        NTH_VALUE(debut_year, 2) OVER (ORDER BY debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_debut_year,
        LAG(debut_year, 2, 0) OVER (PARTITION BY country, genre_or_unknown ORDER BY debut_year, id) AS prev2_debut_year
    FROM scoped
    WINDOW w_is_verified_genre AS (PARTITION BY is_verified, genre), w_country AS (PARTITION BY country), w_country_genre_or_unknown_by_2 AS (PARTITION BY country, genre_or_unknown ORDER BY debut_year, id)
)
-- Final projection
SELECT
    id,
    country,
    debut_year,
    is_verified,
    genre_or_unknown,
    is_verified_genre_max_debut_year,
    country_len_share_of_country,
    distinct_genre_or_unknown_in_country,
    distinct_country_in_is_verified_genre,
    first_known_country_len,
    debut_year_tercile,
    first_known_debut_year,
    distinct_genre_in_country,
    distinct_genre_or_unknown_in_is_verified_genre,
    seq_no,
    second_debut_year,
    prev2_debut_year,
    LEAST(prev2_debut_year, first_known_country_len) AS least_prev2_debut_year_first_known_country_len
FROM metrics
WHERE first_known_country_len <= 9
ORDER BY first_known_debut_year DESC, id DESC NULLS FIRST
LIMIT 5
