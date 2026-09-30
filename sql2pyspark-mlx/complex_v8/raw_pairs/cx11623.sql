WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        CASE WHEN qty >= 256 THEN 'A' WHEN qty >= 109 THEN 'B' ELSE 'C' END AS qty_tier,
        INITCAP(TRIM(move_type)) AS move_type_clean,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE qty <= 275
      AND unit_price >= 200.7
),
with_details_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.*,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM recent_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_parent_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.wh_code AS warehouse_wh_code_2,
        sk.labels AS sku_labels,
        sk.is_active AS sku_is_active
    FROM with_details_warehouse s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE LENGTH(TRIM(w.wh_code)) >= 8
)
SELECT
    warehouse_id,
    sku_category,
    qty_tier,
    ROUND(100 * COUNT(CASE WHEN sku_is_active = FALSE THEN 1 END) / COUNT(*), 1) AS pct_sku_is_active_false,
    COUNT(*) AS n_rows
FROM with_parent_warehouse
GROUP BY ROLLUP(warehouse_id, sku_category, qty_tier)
ORDER BY warehouse_id NULLS LAST, sku_category NULLS LAST, qty_tier NULLS LAST
