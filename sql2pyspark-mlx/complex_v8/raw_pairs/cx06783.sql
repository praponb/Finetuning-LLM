WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty,
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE move_type <> 'TRANSFER'
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.qty_filled,
        w.wh_code AS warehouse_wh_code,
        sk.is_active AS sku_is_active,
        sk.weight_kg AS sku_weight_kg
    FROM scoped s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        qty_filled,
        warehouse_wh_code,
        sku_is_active,
        sku_weight_kg,
        ROUND(AVG(sku_weight_kg) OVER (PARTITION BY sku_is_active ORDER BY qty_filled NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS sku_weight_kg_avg_1_prior,
        ROW_NUMBER() OVER (PARTITION BY sku_is_active ORDER BY qty_filled NULLS LAST, id) AS seq_no,
        MAX(sku_weight_kg) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS sku_weight_kg_max_2_prior,
        COUNT(*) OVER (PARTITION BY sku_id, operator) AS rows_in_sku_id_operator,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY operator, sku_id)) AS distinct_warehouse_id_in_operator_sku_id,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        LAG(moved_at) OVER w_sku_id_sku_is_active_by_qty AS prev_moved_at,
        NTH_VALUE(sku_weight_kg, 2) OVER (PARTITION BY sku_id, sku_is_active ORDER BY qty_filled DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sku_weight_kg
    FROM joined
    WINDOW w_sku_id_sku_is_active_by_qty AS (PARTITION BY sku_id, sku_is_active ORDER BY qty_filled DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    qty_filled,
    warehouse_wh_code,
    sku_is_active,
    sku_weight_kg_avg_1_prior,
    seq_no,
    sku_weight_kg_max_2_prior,
    rows_in_sku_id_operator,
    distinct_warehouse_id_in_operator_sku_id,
    last_known_moved_at,
    prev_moved_at,
    SUBSTRING(TRIM(warehouse_wh_code), 1, 3) AS warehouse_wh_code_prefix
FROM with_windows
