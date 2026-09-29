WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        moved_at + INTERVAL 7 DAYS AS moved_at_plus_7d,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 10 AND 14
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(unit_price_adj) IGNORE NULLS OVER (PARTITION BY warehouse_id, operator ORDER BY days_since_moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_adj,
        MAX(unit_price_adj) OVER (PARTITION BY warehouse_id, operator ORDER BY days_since_moved_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_adj_max_7,
        MAX(qty) OVER w_sku_id_operator AS sku_id_operator_max_qty
    FROM filtered
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator)
)
SELECT *
FROM (
    SELECT operator, move_type, unit_price
    FROM ranked
) src
PIVOT (
    MIN(unit_price) AS min_v, COUNT(unit_price) AS n
    FOR move_type IN ('INBOUND', 'ADJUST', 'TRANSFER')
)
ORDER BY operator
