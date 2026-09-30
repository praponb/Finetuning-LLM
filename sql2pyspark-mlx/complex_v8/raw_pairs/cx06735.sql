WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj
    FROM stock_moves
    WHERE warehouse_id <= 5
),
joined_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.unit_price_adj,
        w.capacity AS warehouse_capacity
    FROM scoped s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
sku_id_stats AS (
    -- Median qty per sku_id
    SELECT sku_id, PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM joined_warehouse
    GROUP BY sku_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_qty, ROUND(s.qty - g.median_qty, 2) AS qty_minus_median
    FROM joined_warehouse s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.qty >= g.median_qty
)
SELECT *
FROM (
    SELECT moved_at, sku_id, operator, unit_price_adj
    FROM outliers
) src
PIVOT (
    COUNT(unit_price_adj)
    FOR operator IN ('op_bee', 'op_ann', 'op_dew', 'op_chan')
)
