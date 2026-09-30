WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        CASE WHEN qty >= 394 THEN 'large' WHEN qty >= 99 THEN 'medium' ELSE 'small' END AS qty_tier,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-02-06 10:00:00'
      AND move_type IS NOT NULL
),
without_matches AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM recent_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.capacity >= 2614
        )
)
SELECT *
FROM (
    SELECT warehouse_id, sku_id, move_type, qty
    FROM without_matches
) src
PIVOT (
    MAX(qty) AS max_v, COUNT(qty) AS n
    FOR move_type IN ('OUTBOUND', 'ADJUST')
)
