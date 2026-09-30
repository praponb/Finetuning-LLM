WITH valid_skus AS (
    -- Filter skus
    SELECT
        id,
        category,
        labels,
        is_active,
        SIZE(labels) AS labels_count,
        LOWER(TRIM(category)) AS category_clean
    FROM skus
    WHERE weight_kg <= 14.3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY is_active, labels_count ORDER BY labels_count DESC) AS labels_count_rank,
        SIZE(COLLECT_SET(category) OVER (PARTITION BY is_active)) AS distinct_category_in_is_active,
        SUM(labels_count) OVER (PARTITION BY labels_count, category ORDER BY labels_count ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS labels_count_sum_3,
        ROUND(labels_count / NULLIF(SUM(labels_count) OVER (PARTITION BY is_active), 0), 4) AS labels_count_share_of_is_active
    FROM valid_skus
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count, COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM with_trends s
    LEFT JOIN move_stats st ON st.sku_id = s.id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM with_counts
    WHERE labels_count < (SELECT MAX(labels_count) FROM with_counts)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        labels_count,
        distinct_category_in_is_active,
        PERCENTILE_APPROX(labels_count, 0.5) AS median_labels_count,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list,
        MAX(move_count_2) AS max_move_count_2,
        COUNT(*) AS n_rows
    FROM qualified
    GROUP BY labels_count, distinct_category_in_is_active
    HAVING COUNT(*) >= 2
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE labels_count > (SELECT MIN(labels_count) FROM rollup_base)
),
scored AS (
    -- Derive helper columns
    SELECT
        labels_count,
        distinct_category_in_is_active,
        max_move_count_2,
        n_rows,
        LEAST(max_move_count_2, median_labels_count) AS least_max_move_count_2_median_labels_count
    FROM without_matches
)
-- Final projection
SELECT
    labels_count,
    distinct_category_in_is_active,
    n_rows,
    least_max_move_count_2_median_labels_count,
    ROUND(n_rows / NULLIF(labels_count, 0), 3) AS n_rows_per_labels_count,
    ABS(n_rows - 8) AS n_rows_distance
FROM scored
ORDER BY distinct_category_in_is_active DESC
