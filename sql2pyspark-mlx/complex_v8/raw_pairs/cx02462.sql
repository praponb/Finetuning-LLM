WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE bpm > 104
),
rollup_base AS (
    -- Summarise per group
    SELECT
        is_explicit,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list
    FROM base
    GROUP BY is_explicit
    HAVING COUNT(*) >= 3
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, n_rows ORDER BY n_rows DESC, is_explicit) AS n_rows_seq,
        DENSE_RANK() OVER w_is_explicit_n_rows_by_n_rows_2 AS n_rows_dense_rank,
        ROUND(AVG(n_rows) OVER w_n_rows, 2) AS n_rows_avg_n_rows
    FROM rollup_base
    WINDOW w_is_explicit_n_rows_by_n_rows_2 AS (PARTITION BY is_explicit, n_rows ORDER BY n_rows), w_n_rows AS (PARTITION BY n_rows)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        is_explicit,
        title_list,
        n_rows_dense_rank,
        n_rows_avg_n_rows,
        ROUND(n_rows_dense_rank / NULLIF(SUM(n_rows_dense_rank) OVER (PARTITION BY n_rows_seq), 0), 4) AS n_rows_dense_rank_share_of_n_rows_seq,
        RANK() OVER (PARTITION BY is_explicit ORDER BY n_rows_avg_n_rows DESC) AS n_rows_avg_n_rows_rank,
        FIRST_VALUE(n_rows_dense_rank) OVER (PARTITION BY is_explicit ORDER BY n_rows_avg_n_rows DESC, is_explicit ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_rows_dense_rank,
        ROUND(n_rows - AVG(n_rows) OVER (PARTITION BY n_rows_seq), 2) AS n_rows_vs_n_rows_seq_avg
    FROM ranked
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        n_rows_avg_n_rows - LAG(n_rows_avg_n_rows) OVER w_n_rows_dense_rank_is_explici AS n_rows_avg_n_rows_change,
        LEAD(n_rows_dense_rank) OVER (PARTITION BY is_explicit ORDER BY n_rows_vs_n_rows_seq_avg, is_explicit) AS next_n_rows_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY first_n_rows_dense_rank DESC), 3) AS first_n_rows_dense_rank_percent_rank,
        ROUND(AVG(first_n_rows_dense_rank) OVER (PARTITION BY n_rows_dense_rank, is_explicit ORDER BY first_n_rows_dense_rank DESC, is_explicit ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS first_n_rows_dense_rank_avg_2_prior
    FROM with_trends
    WINDOW w_n_rows_dense_rank_is_explici AS (PARTITION BY n_rows_dense_rank, is_explicit ORDER BY first_n_rows_dense_rank DESC, is_explicit)
)
SELECT
    is_explicit,
    n_rows_dense_rank,
    n_rows_avg_n_rows,
    n_rows_avg_n_rows_rank,
    first_n_rows_dense_rank,
    next_n_rows_dense_rank
FROM ranked_2
ORDER BY next_n_rows_dense_rank DESC, n_rows_avg_n_rows_rank NULLS LAST
