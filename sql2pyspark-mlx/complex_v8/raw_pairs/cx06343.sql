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
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE qty BETWEEN 124 AND 394
),
combined AS (
    -- Stack the branches
    SELECT operator AS category, qty
    FROM prepared
    WHERE unit_price_bucket BETWEEN 101 AND 400
      AND unit_price >= 180.0
    UNION
    SELECT category, id
    FROM skus
)
SELECT category, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM combined
GROUP BY category
ORDER BY category
