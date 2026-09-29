SELECT
    id AS qty,
    operator AS name,
    warehouse_id AS ref_id,
    'move' AS src
FROM stock_moves
UNION ALL
SELECT
    capacity,
    city,
    id,
    'warehouse' AS src
FROM warehouses
WHERE wh_code IS NOT NULL
ORDER BY ref_id
