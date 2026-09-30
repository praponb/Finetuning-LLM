WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
      AND warehouse_id >= 2
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM valid s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_category), 2) AS unit_price_vs_sku_category_avg,
        NTILE(5) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS moved_at_quintile,
        DENSE_RANK() OVER (ORDER BY moved_at) AS moved_at_dense_rank,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        LEAD(moved_at) OVER (ORDER BY moved_at, id) AS next_moved_at,
        LAG(unit_price, 1, 0) OVER w_all_by_moved_at AS prev_unit_price,
        COUNT(*) OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator), 0), 4) AS unit_price_share_of_operator,
        ROUND(CUME_DIST() OVER w_sku_category_sku_id_by_moved, 3) AS moved_at_cume_dist,
        MAX(unit_price) OVER w_sku_category AS sku_category_max_unit_price,
        ROUND(AVG(unit_price) OVER w_sku_category, 2) AS sku_category_avg_unit_price,
        LAG(moved_at) OVER w_sku_category_sku_id_by_moved_2 AS prev_moved_at,
        RANK() OVER w_move_type_by_moved_at_ties AS moved_at_rank
    FROM with_details
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at, id), w_sku_category_sku_id_by_moved AS (PARTITION BY sku_category, sku_id ORDER BY moved_at DESC), w_sku_category AS (PARTITION BY sku_category), w_sku_category_sku_id_by_moved_2 AS (PARTITION BY sku_category, sku_id ORDER BY moved_at DESC, id), w_move_type_by_moved_at_ties AS (PARTITION BY move_type ORDER BY moved_at NULLS LAST)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        SUM(CASE WHEN move_type IS NOT NULL THEN running_count ELSE 0 END) AS running_count_move_type_is_not_null,
        COUNT(DISTINCT operator) AS distinct_operators,
        ROUND(AVG(unit_price_avg_1_prior), 2) AS avg_unit_price_avg_1_prior,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_trends
    GROUP BY sku_id
)
SELECT
    sku_id,
    distinct_operators,
    avg_unit_price_avg_1_prior,
    row_count
FROM rollup_base
ORDER BY avg_unit_price_avg_1_prior, distinct_operators, sku_id
LIMIT 5
