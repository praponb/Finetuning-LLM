WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        CONCAT(operator, '#', CAST(qty AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE move_type = 'ADJUST'
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.days_since_moved_at,
        s.operator_ref,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category
    FROM scoped s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.category = 'Toys'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(sku_unit_cost) OVER (ORDER BY days_since_moved_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS sku_unit_cost_avg_6_prior,
        ROUND(AVG(sku_unit_cost) OVER (PARTITION BY sku_category, move_type), 2) AS sku_category_move_type_avg_sku_unit_cost,
        ROUND(CUME_DIST() OVER w_all_by_days_since_moved_at_t, 3) AS days_since_moved_at_cume_dist,
        LAG(sku_unit_cost, 1, 0) OVER (PARTITION BY sku_category, move_type ORDER BY days_since_moved_at, id) AS prev_sku_unit_cost,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER (PARTITION BY sku_category, move_type), 0), 4) AS sku_unit_cost_share_of_sku_category_move_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(moved_at), sku_category ORDER BY sku_unit_cost, id), 3) AS sku_unit_cost_percent_rank,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY days_since_moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        MAX(days_since_moved_at) OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_moved_at_so_far,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER w_sku_category_move_type, 0), 4) AS days_since_moved_at_share_of_sku_category_move_t,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_category, move_type ORDER BY days_since_moved_at, id), 3) AS days_since_moved_at_cume_dist_2,
        sku_unit_cost - LAG(sku_unit_cost) OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id) AS sku_unit_cost_change,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY moved_at DESC, id) AS moved_at_dense_rank,
        NTH_VALUE(sku_unit_cost, 2) OVER (PARTITION BY YEAR(moved_at), sku_category ORDER BY sku_unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_unit_cost,
        COUNT(*) OVER w_sku_id AS rows_in_sku_id,
        SUM(sku_unit_cost) OVER (PARTITION BY sku_category, move_type ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sku_unit_cost,
        ROW_NUMBER() OVER w_sku_id_by_moved_at AS seq_no
    FROM combined_details
    WINDOW w_all_by_days_since_moved_at_t AS (ORDER BY days_since_moved_at DESC), w_sku_category_move_type AS (PARTITION BY sku_category, move_type), w_sku_id AS (PARTITION BY sku_id), w_sku_id_by_moved_at AS (PARTITION BY sku_id ORDER BY moved_at DESC, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE seq_no <= 2
      AND warehouse_id >= 4
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(sku_unit_cost_percent_rank - AVG(sku_unit_cost_percent_rank) OVER (PARTITION BY YEAR(moved_at)), 2) AS sku_unit_cost_percent_rank_vs_moved_at_year_avg,
        days_since_moved_at - LAG(days_since_moved_at) OVER w_all_by_moved_at_dense_rank AS days_since_moved_at_change,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY YEAR(moved_at))) AS distinct_move_type_in_moved_at_year,
        LAST_VALUE(sku_unit_cost) IGNORE NULLS OVER (ORDER BY running_sku_unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sku_unit_cost,
        ROUND(AVG(seq_no) OVER (PARTITION BY warehouse_id ORDER BY seq_no ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS seq_no_avg_1_prior,
        MAX(second_sku_unit_cost) OVER (PARTITION BY warehouse_id ORDER BY seq_no ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_second_sku_unit_cost_so_far,
        NTILE(10) OVER (ORDER BY running_sku_unit_cost DESC, id) AS running_sku_unit_cost_decile
    FROM top_rows
    WINDOW w_all_by_moved_at_dense_rank AS (ORDER BY moved_at_dense_rank, id)
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    moved_at,
    days_since_moved_at,
    operator_ref,
    sku_unit_cost,
    sku_category,
    sku_unit_cost_share_of_sku_category_move_type,
    sku_unit_cost_percent_rank,
    last_known_moved_at,
    days_since_moved_at_share_of_sku_category_move_t,
    days_since_moved_at_cume_dist_2,
    sku_unit_cost_change,
    moved_at_dense_rank,
    second_sku_unit_cost,
    rows_in_sku_id,
    running_sku_unit_cost,
    seq_no,
    distinct_move_type_in_moved_at_year,
    last_known_sku_unit_cost,
    seq_no_avg_1_prior,
    max_second_sku_unit_cost_so_far,
    GREATEST(prev_sku_unit_cost, sku_unit_cost) AS greatest_prev_sku_unit_cost_sku_unit_cost
FROM ranked
ORDER BY move_type NULLS LAST, id
LIMIT 5
