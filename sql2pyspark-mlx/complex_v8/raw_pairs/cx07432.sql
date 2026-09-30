WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        genre,
        country,
        debut_year,
        is_verified,
        SUBSTRING(TRIM(genre), 1, 3) AS genre_prefix,
        ABS(debut_year - 2006) AS debut_year_distance,
        LENGTH(TRIM(country)) AS country_len
    FROM artists
    WHERE debut_year >= 1994
),
stats AS (
    -- Group-level totals
    SELECT
        is_verified,
        genre,
        ROUND(AVG(debut_year), 2) AS avg_debut_year,
        ARRAY_SORT(COLLECT_SET(country)) AS country_list,
        ROUND(100 * COUNT(CASE WHEN genre = 'Indie' THEN 1 END) / COUNT(*), 1) AS pct_genre_indie
    FROM cleaned
    GROUP BY 1, 2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY genre ORDER BY pct_genre_indie DESC) AS pct_genre_indie_dense_rank,
        ROUND(pct_genre_indie / NULLIF(SUM(pct_genre_indie) OVER (PARTITION BY is_verified), 0), 4) AS pct_genre_indie_share_of_is_verified,
        LEAD(avg_debut_year, 2) OVER (PARTITION BY genre ORDER BY pct_genre_indie DESC, is_verified, genre) AS next2_avg_debut_year,
        LAST_VALUE(pct_genre_indie) IGNORE NULLS OVER (PARTITION BY genre ORDER BY pct_genre_indie DESC, is_verified, genre ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pct_genre_indie
    FROM stats
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_all_by_pct_genre_indie_share AS pct_genre_indie_share_of_is_verified_rank,
        ROUND(AVG(pct_genre_indie) OVER (ORDER BY pct_genre_indie_share_of_is_verified DESC, is_verified, genre ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS pct_genre_indie_avg_7,
        SUM(pct_genre_indie_dense_rank) OVER (ORDER BY pct_genre_indie_share_of_is_verified DESC, is_verified, genre ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS pct_genre_indie_dense_rank_sum_3_prior,
        COUNT(*) OVER w_is_verified_pct_genre_indie AS rows_in_is_verified_pct_genre_indie_dense_rank
    FROM metrics
    WINDOW w_all_by_pct_genre_indie_share AS (ORDER BY pct_genre_indie_share_of_is_verified DESC, is_verified, genre), w_is_verified_pct_genre_indie AS (PARTITION BY is_verified, pct_genre_indie_dense_rank)
),
annotated AS (
    -- Derive helper columns
    SELECT
        is_verified,
        genre,
        avg_debut_year,
        country_list,
        pct_genre_indie,
        pct_genre_indie_dense_rank,
        pct_genre_indie_share_of_is_verified,
        next2_avg_debut_year,
        last_known_pct_genre_indie,
        pct_genre_indie_share_of_is_verified_rank,
        pct_genre_indie_avg_7,
        pct_genre_indie_dense_rank_sum_3_prior,
        rows_in_is_verified_pct_genre_indie_dense_rank,
        SUBSTRING(TRIM(genre), 1, 2) AS genre_prefix
    FROM with_trends
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(pct_genre_indie) OVER w_pct_genre_indie_dense_rank, 2) AS pct_genre_indie_dense_rank_avg_pct_genre_indie,
        MAX(pct_genre_indie) OVER w_pct_genre_indie_dense_rank AS pct_genre_indie_dense_rank_max_pct_genre_indie
    FROM annotated
    WINDOW w_pct_genre_indie_dense_rank AS (PARTITION BY pct_genre_indie_dense_rank)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        IF(is_verified, 'yes', 'no') AS is_verified_label,
        CASE WHEN genre IN ('Pop', 'Jazz') THEN 'group_a' WHEN genre = 'Classical' THEN 'group_b' ELSE 'other' END AS genre_group,
        ROUND(avg_debut_year * 0.9, 2) AS avg_debut_year_adj
    FROM sequenced
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT genre, genre_group, pct_genre_indie_share_of_is_verified
    FROM derived
) src
PIVOT (
    COUNT(pct_genre_indie_share_of_is_verified)
    FOR genre_group IN ('group_b', 'group_a')
)
ORDER BY genre
