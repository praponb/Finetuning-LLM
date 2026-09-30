WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-18 15:00:00'
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CASE WHEN qty >= 245 THEN 'large' WHEN qty >= 208 THEN 'medium' ELSE 'small' END AS qty_tier
    FROM base
),
annotated_2 AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        qty_tier,
        DATE_TRUNC('QUARTER', moved_at) AS moved_at_quarter,
        CONCAT_WS(' / ', move_type, qty_tier) AS move_type_qty_tier_label
    FROM annotated
    WHERE move_type = 'INBOUND'
)
SELECT *
FROM (
    SELECT sku_id, moved_at_quarter, move_type, qty
    FROM annotated_2
) src
PIVOT (
    SUM(qty)
    FOR move_type IN ('OUTBOUND', 'ADJUST', 'INBOUND', 'TRANSFER')
)
