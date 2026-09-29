WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND moved_at < TIMESTAMP '2025-02-12 13:00:00'
),
move_type_stats AS (
    -- Avg qty per move_type
    SELECT move_type, AVG(qty) AS avg_qty
    FROM prepared
    GROUP BY move_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_qty, ROUND(s.qty - g.avg_qty, 2) AS qty_minus_avg
    FROM prepared s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.qty > g.avg_qty
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.unit_price_bucket,
        s.avg_qty,
        s.qty_minus_avg,
        sk.category AS sku_category,
        sk.sku_code
    FROM outliers s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        unit_price,
        unit_price_bucket,
        avg_qty,
        sku_category,
        sku_code,
        ROUND(avg_qty / NULLIF(qty_minus_avg, 0), 3) AS avg_qty_per_qty_minus_avg,
        CONCAT(sku_category, '#', CAST(sku_id AS STRING)) AS sku_category_ref
    FROM with_details_sku
),
enriched_2 AS (
    -- Attach warehouse attributes
    SELECT s.*, w.opened_on AS warehouse_opened_on
    FROM enriched s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    id,
    sku_id,
    avg_qty,
    sku_category,
    sku_code,
    avg_qty_per_qty_minus_avg,
    sku_category_ref,
    warehouse_opened_on,
    LENGTH(TRIM(sku_code)) AS sku_code_len
FROM enriched_2
WHERE warehouse_opened_on < DATE '2025-02-14'
ORDER BY sku_code_len NULLS LAST, warehouse_opened_on DESC
