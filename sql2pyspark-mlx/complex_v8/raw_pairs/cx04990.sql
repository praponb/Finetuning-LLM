WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        COALESCE(qty, 0) AS qty_filled
    FROM stock_moves
    WHERE warehouse_id <= 3
      AND unit_price < 330.9
),
with_parent_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.qty_filled,
        sk.unit_cost AS sku_unit_cost
    FROM valid s
    JOIN skus sk ON sk.id = s.sku_id AND sk.category IN ('Electronics', 'Beauty', 'Toys')
    WHERE sk.unit_cost >= 38.3
)
-- Subtotals with ROLLUP
SELECT
    warehouse_id,
    sku_id,
    GROUPING(warehouse_id) AS is_total_warehouse_id,
    PERCENTILE_APPROX(qty, 0.5) AS median_qty,
    MIN(sku_unit_cost) AS min_sku_unit_cost
FROM with_parent_sku
GROUP BY ROLLUP(warehouse_id, sku_id)
ORDER BY warehouse_id NULLS LAST, sku_id NULLS LAST
