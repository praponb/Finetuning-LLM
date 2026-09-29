WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price
    FROM stock_moves
    WHERE warehouse_id >= 3
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        sk.sku_code
    FROM valid_moves s
    JOIN skus sk ON sk.id = s.sku_id AND sk.weight_kg <= 15.2
    WHERE LOWER(sk.sku_code) LIKE '%10%'
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    sku_code
FROM enriched
ORDER BY id, sku_code
LIMIT 10
