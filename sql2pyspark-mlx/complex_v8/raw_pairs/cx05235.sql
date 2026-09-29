WITH filtered AS (
    -- Filter artists
    SELECT
        id,
        genre,
        country,
        debut_year,
        CONCAT_WS('|', artist_name, genre) AS artist_name_genre_label,
        CONCAT(genre, '#', CAST(debut_year AS STRING)) AS genre_ref
    FROM artists
    WHERE country <> 'BR'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY genre, country ORDER BY debut_year DESC, id) AS debut_year_rank,
        SUM(debut_year) OVER (PARTITION BY country, genre) AS country_genre_sum_debut_year,
        RANK() OVER (PARTITION BY genre, country ORDER BY debut_year DESC) AS debut_year_rank_2,
        LAG(debut_year) OVER (PARTITION BY genre, country ORDER BY debut_year DESC, id) AS prev_debut_year
    FROM filtered
    WINDOW w_country_genre AS (PARTITION BY country, genre)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        genre,
        country,
        artist_name_genre_label,
        debut_year_rank,
        country_genre_sum_debut_year,
        debut_year_rank_2,
        ROUND(AVG(debut_year) OVER (PARTITION BY genre, debut_year_rank ORDER BY debut_year DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS debut_year_avg_6_prior,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY debut_year_rank_2)) AS distinct_country_in_debut_year_rank_2,
        ROUND(debut_year - AVG(debut_year) OVER (PARTITION BY debut_year_rank_2), 2) AS debut_year_vs_debut_year_rank_2_avg,
        DENSE_RANK() OVER (PARTITION BY genre, debut_year_rank ORDER BY debut_year DESC) AS debut_year_dense_rank
    FROM with_trends
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(debut_year_rank) OVER (PARTITION BY distinct_country_in_debut_year_rank_2 ORDER BY debut_year_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_debut_year_rank,
        ROUND(AVG(country_genre_sum_debut_year) OVER w_debut_year_dense_rank, 2) AS debut_year_dense_rank_avg_country_genre_sum_debu,
        LEAD(debut_year_dense_rank, 2) OVER w_distinct_country_in_debut_ye_2 AS next2_debut_year_dense_rank,
        LAST_VALUE(debut_year_rank_2) IGNORE NULLS OVER (PARTITION BY distinct_country_in_debut_year_rank_2 ORDER BY debut_year_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_debut_year_rank_2,
        COUNT(*) OVER (PARTITION BY debut_year_dense_rank) AS rows_in_debut_year_dense_rank,
        debut_year_rank_2 - LAG(debut_year_rank_2) OVER (PARTITION BY distinct_country_in_debut_year_rank_2 ORDER BY debut_year_rank DESC, id) AS debut_year_rank_2_change
    FROM with_windows
    WINDOW w_debut_year_dense_rank AS (PARTITION BY debut_year_dense_rank), w_distinct_country_in_debut_ye_2 AS (PARTITION BY distinct_country_in_debut_year_rank_2 ORDER BY debut_year_rank DESC, id)
),
grouped AS (
    -- Summarise per group
    SELECT
        debut_year_rank,
        debut_year_dense_rank,
        PERCENTILE_APPROX(debut_year_rank, 0.5) AS median_debut_year_rank,
        MAX(debut_year_rank_2) AS max_debut_year_rank_2,
        COUNT(DISTINCT genre) AS distinct_genres,
        SUM(CASE WHEN country = 'BR' THEN debut_year_avg_6_prior ELSE 0 END) AS debut_year_avg_6_prior_country_br
    FROM metrics
    GROUP BY debut_year_rank, debut_year_dense_rank
    HAVING AVG(debut_year_rank) > 2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        debut_year_rank,
        debut_year_dense_rank,
        median_debut_year_rank,
        max_debut_year_rank_2,
        distinct_genres,
        debut_year_avg_6_prior_country_br,
        ROUND(distinct_genres / NULLIF(SUM(distinct_genres) OVER (PARTITION BY median_debut_year_rank, distinct_genres), 0), 4) AS distinct_genres_share_of_median_debut_year_rank_,
        DENSE_RANK() OVER (PARTITION BY max_debut_year_rank_2 ORDER BY median_debut_year_rank ASC, debut_year_rank, debut_year_dense_rank) AS median_debut_year_rank_dense_rank,
        DENSE_RANK() OVER (PARTITION BY max_debut_year_rank_2 ORDER BY median_debut_year_rank ASC) AS median_debut_year_rank_dense_rank_2
    FROM grouped
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM sequenced
    WHERE debut_year_dense_rank > (SELECT MIN(debut_year_dense_rank) FROM sequenced)
)
-- Final projection
SELECT
    debut_year_rank,
    median_debut_year_rank,
    max_debut_year_rank_2,
    distinct_genres,
    median_debut_year_rank_dense_rank,
    median_debut_year_rank_dense_rank_2,
    ABS(median_debut_year_rank - 6) AS median_debut_year_rank_distance
FROM without_matches
WHERE max_debut_year_rank_2 <= 9
ORDER BY max_debut_year_rank_2 DESC, median_debut_year_rank_distance NULLS LAST
