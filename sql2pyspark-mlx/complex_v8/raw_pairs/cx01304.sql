WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE move_type <> 'TRANSFER'
      AND operator IS NOT NULL
),
with_parent AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        w.capacity AS warehouse_capacity,
        sk.sku_code
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
sku_id_stats AS (
    -- Median qty per sku_id
    SELECT sku_id, PERCENTILE_APPROX(qty, 0.5) AS median_qty
    FROM with_parent
    GROUP BY sku_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_qty, ROUND(s.qty - g.median_qty, 2) AS qty_minus_median
    FROM with_parent s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.qty >= g.median_qty
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    sku_code,
    median_qty,
    qty_minus_median,
    LENGTH(TRIM(sku_code)) AS sku_code_len
FROM compared
ORDER BY qty
