WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
unioned AS (
    -- Stack the branches
    SELECT id, warehouse_id AS key_id, move_type AS category
    FROM base
    UNION ALL
    SELECT capacity, id, wh_code
    FROM warehouses
    WHERE capacity BETWEEN 2614 AND 11506
      AND UPPER(TRIM(wh_code)) LIKE 'B%'
    UNION ALL
    SELECT warehouse_id, qty, operator
    FROM stock_moves
    WHERE unit_price <= 337.6
      AND operator = 'op_eve'
)
SELECT category, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM unioned
GROUP BY category
ORDER BY category
