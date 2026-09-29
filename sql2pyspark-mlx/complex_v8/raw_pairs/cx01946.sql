WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        ABS(bpm - 143) AS bpm_distance
    FROM tracks
    WHERE is_explicit = FALSE
),
stats AS (
    -- Group-level totals
    SELECT
        is_explicit,
        SUM(CASE WHEN is_explicit = TRUE THEN bpm_distance ELSE 0 END) AS bpm_distance_is_explicit_true,
        SUM(bpm_distance) FILTER (WHERE is_explicit = TRUE) AS bpm_distance_is_explicit_true_2
    FROM valid
    GROUP BY 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_is_explicit_by_bpm_distance AS bpm_distance_is_explicit_true_rank,
        RANK() OVER w_is_explicit_by_bpm_distance_2 AS bpm_distance_is_explicit_true_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY bpm_distance_is_explicit_true DESC), 3) AS bpm_distance_is_explicit_true_cume_dist,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY bpm_distance_is_explicit_true DESC) AS bpm_distance_is_explicit_true_dense_rank
    FROM stats
    WINDOW w_is_explicit_by_bpm_distance AS (PARTITION BY is_explicit ORDER BY bpm_distance_is_explicit_true DESC, is_explicit), w_is_explicit_by_bpm_distance_2 AS (PARTITION BY is_explicit ORDER BY bpm_distance_is_explicit_true DESC)
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM with_windows
    WHERE bpm_distance_is_explicit_true_rank_2 > (SELECT MIN(bpm_distance_is_explicit_true_rank_2) FROM with_windows)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        is_explicit,
        bpm_distance_is_explicit_true_rank,
        bpm_distance_is_explicit_true_dense_rank,
        ROUND(PERCENT_RANK() OVER w_is_explicit_bpm_distance_is, 3) AS bpm_distance_is_explicit_true_rank_percent_rank,
        ROUND(CUME_DIST() OVER w_bpm_distance_is_explicit_tru, 3) AS bpm_distance_is_explicit_true_cume_dist_cume_dis,
        NTILE(3) OVER w_bpm_distance_is_explicit_tru AS bpm_distance_is_explicit_true_cume_dist_tercile,
        RANK() OVER (PARTITION BY bpm_distance_is_explicit_true_dense_rank, bpm_distance_is_explicit_true_rank ORDER BY bpm_distance_is_explicit_true_cume_dist ASC, is_explicit) AS bpm_distance_is_explicit_true_cume_dist_rank,
        ROW_NUMBER() OVER (PARTITION BY bpm_distance_is_explicit_true_dense_rank, bpm_distance_is_explicit_true_rank ORDER BY bpm_distance_is_explicit_true_cume_dist ASC, is_explicit) AS rn,
        RANK() OVER (PARTITION BY is_explicit, bpm_distance_is_explicit_true_dense_rank ORDER BY bpm_distance_is_explicit_true_rank DESC) AS bpm_distance_is_explicit_true_rank_rank
    FROM qualified
    WINDOW w_is_explicit_bpm_distance_is AS (PARTITION BY is_explicit, bpm_distance_is_explicit_true_dense_rank ORDER BY bpm_distance_is_explicit_true_rank DESC, is_explicit), w_bpm_distance_is_explicit_tru AS (PARTITION BY bpm_distance_is_explicit_true_dense_rank, bpm_distance_is_explicit_true_rank ORDER BY bpm_distance_is_explicit_true_cume_dist ASC, is_explicit)
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        is_explicit,
        bpm_distance_is_explicit_true_rank,
        bpm_distance_is_explicit_true_dense_rank,
        bpm_distance_is_explicit_true_rank_percent_rank,
        bpm_distance_is_explicit_true_cume_dist_cume_dis,
        bpm_distance_is_explicit_true_cume_dist_tercile,
        bpm_distance_is_explicit_true_cume_dist_rank,
        rn,
        ROW_NUMBER() OVER (ORDER BY rn NULLS LAST, is_explicit) AS rn_seq,
        ROUND(AVG(rn) OVER (PARTITION BY bpm_distance_is_explicit_true_rank_rank), 2) AS bpm_distance_is_explicit_true_rank_rank_avg_rn
    FROM ranked
)
-- Final projection
SELECT
    is_explicit,
    bpm_distance_is_explicit_true_dense_rank,
    bpm_distance_is_explicit_true_rank_percent_rank,
    bpm_distance_is_explicit_true_cume_dist_cume_dis,
    bpm_distance_is_explicit_true_cume_dist_tercile,
    bpm_distance_is_explicit_true_cume_dist_rank,
    rn,
    rn_seq,
    bpm_distance_is_explicit_true_rank_rank_avg_rn,
    FLOOR(rn_seq / 2) * 2 AS rn_seq_bucket
FROM with_windows_2
ORDER BY rn_seq DESC, is_explicit
LIMIT 10
