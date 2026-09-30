WITH prepared_artists AS (
    -- Filter artists
    SELECT *
    FROM artists
    WHERE country NOT IN ('TH', 'US')
),
latest_per_key AS (
    -- Keep the earliest row per country, genre
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country, genre ORDER BY debut_year, id) AS row_num
        FROM prepared_artists
    ) ranked
    WHERE row_num = 1
),
rollup_base AS (
    -- Group-level totals
    SELECT
        genre,
        country,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(debut_year) FILTER (WHERE artist_name IS NOT NULL) AS debut_year_artist_name_is_not_null,
        MAX(debut_year) AS max_debut_year,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN country IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_country_is_not_null,
        SUM(debut_year) AS total_debut_year
    FROM latest_per_key
    GROUP BY 1, 2
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM rollup_base
    WHERE pct_country_is_not_null < (SELECT MAX(pct_country_is_not_null) FROM rollup_base)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        genre,
        country,
        debut_year_artist_name_is_not_null,
        cnt,
        pct_country_is_not_null,
        total_debut_year,
        ROUND(distinct_ids - AVG(distinct_ids) OVER (PARTITION BY distinct_ids, genre), 2) AS distinct_ids_vs_distinct_ids_genre_avg,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY distinct_ids, genre)) AS distinct_country_in_distinct_ids_genre,
        NTH_VALUE(distinct_ids, 2) OVER (PARTITION BY cnt, country ORDER BY distinct_ids ASC, genre, country ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids,
        NTILE(10) OVER (PARTITION BY cnt, country ORDER BY distinct_ids ASC, genre, country) AS distinct_ids_decile,
        FIRST_VALUE(cnt) IGNORE NULLS OVER (PARTITION BY cnt, country ORDER BY distinct_ids ASC, genre, country ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cnt,
        LAG(total_debut_year) OVER (PARTITION BY cnt, country ORDER BY distinct_ids ASC, genre, country) AS prev_total_debut_year
    FROM without_matches
)
-- Final projection
SELECT
    genre,
    country,
    debut_year_artist_name_is_not_null,
    cnt,
    total_debut_year,
    distinct_ids_vs_distinct_ids_genre_avg,
    distinct_country_in_distinct_ids_genre,
    second_distinct_ids,
    distinct_ids_decile,
    first_known_cnt,
    prev_total_debut_year,
    FLOOR(first_known_cnt / 2) * 2 AS first_known_cnt_bucket,
    LOWER(REGEXP_REPLACE(TRIM(genre), ' ', '_')) AS genre_slug
FROM with_windows
ORDER BY genre_slug NULLS LAST, first_known_cnt_bucket NULLS LAST
