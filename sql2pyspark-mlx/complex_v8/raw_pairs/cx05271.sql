WITH filtered_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price
    FROM stock_moves
    WHERE unit_price <= 290.4
),
screened AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM filtered_moves s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.category <> 'Toys'
        )
),
joined AS (
    -- Attach warehouse attributes
    SELECT s.*, w.opened_on AS warehouse_opened_on
    FROM screened s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        qty,
        warehouse_opened_on,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM joined
)
-- Final projection
SELECT
    id,
    warehouse_id,
    qty,
    warehouse_opened_on,
    unit_price_bucket,
    DATE_FORMAT(warehouse_opened_on, 'yyyy-MM-dd') AS warehouse_opened_on_label
FROM enriched
ORDER BY warehouse_opened_on_label DESC NULLS FIRST
