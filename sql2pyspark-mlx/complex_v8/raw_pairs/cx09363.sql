WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        LOWER(REGEXP_REPLACE(TRIM(artist_name), ' ', '_')) AS artist_name_slug
    FROM artists
    WHERE NOT is_verified
      AND debut_year > 1994
),
deduped AS (
    -- Keep the latest row per country
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        artist_name_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY debut_year DESC, id DESC) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
grouped AS (
    -- Group-level totals
    SELECT
        country,
        genre,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN country IN ('KR', 'BR') THEN 1 END) / COUNT(*), 1) AS pct_country_in_kr_br,
        COUNT(DISTINCT country) AS distinct_countrys,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT genre) AS distinct_genres
    FROM deduped
    WHERE artist_name_slug IS NOT NULL
    GROUP BY country, genre
    HAVING SUM(debut_year) > 1997
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY distinct_genres DESC) AS distinct_genres_dense_rank,
        NTH_VALUE(distinct_ids, 2) OVER (ORDER BY distinct_genres DESC, country, genre ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids,
        ROUND(AVG(distinct_ids) OVER (ORDER BY distinct_genres DESC, country, genre ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS distinct_ids_avg_1_prior,
        ROUND(distinct_genres - AVG(distinct_genres) OVER (PARTITION BY genre), 2) AS distinct_genres_vs_genre_avg,
        RANK() OVER w_all_by_distinct_genres AS distinct_genres_rank
    FROM grouped
    WINDOW w_all_by_distinct_genres AS (ORDER BY distinct_genres DESC, country, genre)
),
scored AS (
    -- Add calculated columns
    SELECT *, FLOOR(pct_country_in_kr_br / 25) * 25 AS pct_country_in_kr_br_bucket
    FROM metrics
    WHERE distinct_ids <= 9
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(pct_country_in_kr_br_bucket) OVER w_distinct_ids, 2) AS distinct_ids_avg_pct_country_in_kr_br_bucket,
        NTILE(4) OVER (PARTITION BY distinct_genres_dense_rank, second_distinct_ids ORDER BY distinct_genres_dense_rank DESC, country, genre) AS distinct_genres_dense_rank_quartile,
        NTH_VALUE(distinct_ids, 2) OVER (PARTITION BY distinct_genres_dense_rank, second_distinct_ids ORDER BY distinct_genres_dense_rank DESC, country, genre ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids_2,
        ROUND(distinct_genres_rank / NULLIF(SUM(distinct_genres_rank) OVER w_distinct_ids, 0), 4) AS distinct_genres_rank_share_of_distinct_ids,
        LAST_VALUE(distinct_genres_dense_rank) OVER (PARTITION BY distinct_genres_dense_rank, second_distinct_ids ORDER BY distinct_genres_dense_rank DESC, country, genre ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_genres_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_genres_dense_rank, second_distinct_ids ORDER BY distinct_genres_dense_rank DESC), 3) AS distinct_genres_dense_rank_cume_dist
    FROM scored
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids)
)
-- Final projection
SELECT
    country,
    genre,
    row_count,
    pct_country_in_kr_br,
    distinct_countrys,
    distinct_genres,
    distinct_genres_dense_rank,
    second_distinct_ids,
    distinct_ids_avg_1_prior,
    distinct_genres_vs_genre_avg,
    distinct_genres_rank,
    pct_country_in_kr_br_bucket,
    second_distinct_ids_2,
    last_distinct_genres_dense_rank
FROM metrics_2
ORDER BY distinct_countrys, pct_country_in_kr_br DESC
