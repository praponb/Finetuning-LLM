WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE (qty <= 355 OR operator NOT IN ('op_eve', 'op_ann'))
),
combined AS (
    -- Stack the branches
    SELECT 'move' AS src, sku_id AS entity_id, warehouse_id AS qty
    FROM prepared
    UNION ALL
    SELECT 'move_2' AS src, id, warehouse_id
    FROM stock_moves
)
SELECT src, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM combined
GROUP BY src
ORDER BY src
