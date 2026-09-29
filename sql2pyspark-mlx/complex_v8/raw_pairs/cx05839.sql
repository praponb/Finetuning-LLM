WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE operator IN ('op_dew', 'op_ann')
      AND moved_at >= TIMESTAMP '2025-02-06 07:00:00'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        sk.labels AS sku_labels,
        sk.sku_code
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
exploded AS (
    -- One row per sku_label
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.warehouse_city,
        s.warehouse_wh_code,
        s.warehouse_opened_on,
        s.sku_code,
        t.sku_label
    FROM combined_details s
    LATERAL VIEW EXPLODE(s.sku_labels) t AS sku_label
),
rollup_base AS (
    -- Group-level totals
    SELECT
        warehouse_city,
        COUNT(*) FILTER (WHERE warehouse_wh_code IS NOT NULL) AS n_warehouse_wh_code_is_not_nul,
        ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list,
        MAX(unit_price) AS max_unit_price
    FROM exploded
    WHERE sku_id IS NOT NULL
    GROUP BY warehouse_city
),
labeled AS (
    -- Derive helper columns
    SELECT
        warehouse_city,
        n_warehouse_wh_code_is_not_nul,
        ROUND(max_unit_price * 0.25, 2) AS max_unit_price_adj
    FROM rollup_base
)
-- Final projection
SELECT
    warehouse_city,
    n_warehouse_wh_code_is_not_nul,
    max_unit_price_adj,
    CASE WHEN max_unit_price_adj >= 487.9 THEN 'HIGH' WHEN max_unit_price_adj >= 263.1 THEN 'MEDIUM' ELSE 'LOW' END AS max_unit_price_adj_tier,
    COALESCE(max_unit_price_adj, 0.0) AS max_unit_price_adj_filled
FROM labeled
ORDER BY max_unit_price_adj, warehouse_city
LIMIT 10
