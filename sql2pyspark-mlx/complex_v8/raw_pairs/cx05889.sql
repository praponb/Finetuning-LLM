WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        DATE_TRUNC('WEEK', release_date) AS release_date_week
    FROM tracks
),
rollup_base AS (
    -- Group-level totals
    SELECT
        is_explicit_flag,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        SUM(CASE WHEN is_explicit = TRUE THEN 1 ELSE 0 END) AS n_is_explicit_true,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        MIN(release_date_week) AS first_release_date_week
    FROM scoped
    WHERE artist_id IS NOT NULL
    GROUP BY is_explicit_flag
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE is_explicit_flag > (SELECT AVG(is_explicit_flag) FROM rollup_base)
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM screened
    WHERE distinct_artist_ids > (SELECT MIN(distinct_artist_ids) FROM screened)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        is_explicit_flag,
        first_release_date_week,
        ROUND(is_explicit_flag / NULLIF(SUM(is_explicit_flag) OVER (PARTITION BY is_explicit_flag), 0), 4) AS is_explicit_flag_share_of_is_explicit_flag,
        FIRST_VALUE(distinct_artist_ids) OVER (ORDER BY n_is_explicit_true ASC, is_explicit_flag ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_artist_ids,
        SUM(distinct_artist_ids) OVER (PARTITION BY n_is_explicit_true, is_explicit_flag ORDER BY distinct_artist_ids DESC, is_explicit_flag ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS distinct_artist_ids_sum_4,
        ROUND(CUME_DIST() OVER w_n_is_explicit_true_is_explic_2, 3) AS distinct_artist_ids_cume_dist,
        MAX(n_is_explicit_true) OVER (ORDER BY n_is_explicit_true ASC, is_explicit_flag ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS n_is_explicit_true_max_6_prior,
        ROW_NUMBER() OVER (ORDER BY n_is_explicit_true ASC, is_explicit_flag) AS n_is_explicit_true_seq
    FROM qualified
    WINDOW w_n_is_explicit_true_is_explic_2 AS (PARTITION BY n_is_explicit_true, is_explicit_flag ORDER BY distinct_artist_ids DESC, is_explicit_flag)
),
top_per_group AS (
    -- First row per group
    SELECT
        is_explicit_flag,
        first_release_date_week,
        is_explicit_flag_share_of_is_explicit_flag,
        first_distinct_artist_ids,
        distinct_artist_ids_sum_4,
        distinct_artist_ids_cume_dist,
        n_is_explicit_true_max_6_prior
    FROM sequenced
    WHERE n_is_explicit_true_seq = 1
)
-- Final projection
SELECT
    is_explicit_flag,
    first_release_date_week,
    is_explicit_flag_share_of_is_explicit_flag,
    first_distinct_artist_ids,
    distinct_artist_ids_sum_4,
    distinct_artist_ids_cume_dist
FROM top_per_group
ORDER BY is_explicit_flag DESC
