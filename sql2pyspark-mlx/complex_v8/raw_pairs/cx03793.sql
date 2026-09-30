WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE unit_price >= 125.9
      AND moved_at >= TIMESTAMP '2025-02-06 12:00:00'
),
rollup_base AS (
    -- Summarise per group
    SELECT
        operator,
        warehouse_id,
        sku_id,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        SUM(CASE WHEN operator <> 'op_ann' THEN qty ELSE 0 END) AS qty_operator_op_ann,
        ROUND(SUM(unit_price) / NULLIF(SUM(qty), 0), 3) AS unit_price_to_qty_ratio,
        SUM(CASE WHEN operator = 'op_dew' THEN 1 ELSE 0 END) AS n_operator_op_dew
    FROM recent_moves
    GROUP BY 1, 2, 3
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.operator,
        s.warehouse_id,
        s.sku_id,
        s.operator_list,
        s.qty_operator_op_ann,
        s.unit_price_to_qty_ratio,
        s.n_operator_op_dew,
        sk.is_active AS sku_is_active,
        sk.sku_code
    FROM rollup_base s
    JOIN skus sk ON sk.id = s.sku_id AND sk.category = 'Toys'
)
-- Final projection
SELECT
    operator,
    warehouse_id,
    operator_list,
    unit_price_to_qty_ratio
FROM combined_details
ORDER BY unit_price_to_qty_ratio DESC, operator NULLS LAST
