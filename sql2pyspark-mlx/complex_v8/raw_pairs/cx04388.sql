WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS(': ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 3)
      AND warehouse_id <= 2
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM base s
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(sku_unit_cost) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS sku_unit_cost_max_1_prior,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY warehouse_id, sku_id)) AS distinct_move_type_in_warehouse_id_sku_id,
        LAG(unit_price, 2) OVER w_all_by_unit_price AS prev2_unit_price,
        MIN(unit_price) OVER w_operator AS operator_min_unit_price,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_id, sku_id), 2) AS warehouse_id_sku_id_avg_qty,
        ROUND(qty / NULLIF(SUM(qty) OVER w_operator, 0), 4) AS qty_share_of_operator,
        NTH_VALUE(qty, 2) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        FIRST_VALUE(qty) IGNORE NULLS OVER (ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty
    FROM with_details_sku
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price ASC, id), w_operator AS (PARTITION BY operator)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_move_type_sku_category_by_mo AS moved_at_dense_rank,
        ROUND(prev2_unit_price - AVG(prev2_unit_price) OVER (PARTITION BY distinct_move_type_in_warehouse_id_sku_id, sku_id), 2) AS prev2_unit_price_vs_distinct_move_type_in_wareho,
        ROUND(sku_unit_cost_max_1_prior / NULLIF(SUM(sku_unit_cost_max_1_prior) OVER w_distinct_move_type_in_wareho, 0), 4) AS sku_unit_cost_max_1_prior_share_of_distinct_move,
        ROUND(prev2_unit_price / NULLIF(SUM(prev2_unit_price) OVER w_distinct_move_type_in_wareho, 0), 4) AS prev2_unit_price_share_of_distinct_move_type_in_,
        SIZE(COLLECT_SET(operator) OVER w_distinct_move_type_in_wareho) AS distinct_operator_in_distinct_move_type_in_wareh,
        MIN(sku_unit_cost_max_1_prior) OVER (PARTITION BY move_type, sku_category ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_sku_unit_cost_max_1_prior_so_far,
        unit_price - LAG(unit_price) OVER (ORDER BY warehouse_id_sku_id_avg_qty DESC, id) AS unit_price_change,
        SIZE(COLLECT_SET(warehouse_id) OVER w_distinct_move_type_in_wareho) AS distinct_warehouse_id_in_distinct_move_type_in_w
    FROM sequenced
    WINDOW w_move_type_sku_category_by_mo AS (PARTITION BY move_type, sku_category ORDER BY moved_at ASC), w_distinct_move_type_in_wareho AS (PARTITION BY distinct_move_type_in_warehouse_id_sku_id, sku_id)
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    operator,
    unit_price,
    operator_move_type_label,
    sku_unit_cost,
    sku_labels,
    sku_category,
    sku_unit_cost_max_1_prior,
    prev2_unit_price,
    second_qty,
    first_known_qty,
    moved_at_dense_rank,
    prev2_unit_price_vs_distinct_move_type_in_wareho,
    sku_unit_cost_max_1_prior_share_of_distinct_move,
    prev2_unit_price_share_of_distinct_move_type_in_,
    distinct_operator_in_distinct_move_type_in_wareh,
    min_sku_unit_cost_max_1_prior_so_far,
    distinct_warehouse_id_in_distinct_move_type_in_w,
    CASE WHEN operator IN ('op_chan', 'op_dew') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group
FROM metrics
WHERE unit_price >= 152.7
ORDER BY moved_at_dense_rank DESC
