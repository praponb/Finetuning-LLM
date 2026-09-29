WITH all_events AS (
    -- Stack the branches
    SELECT id, wh_code, 'warehouse' AS src
    FROM warehouses
    UNION ALL
    SELECT sku_id AS id, operator AS wh_code, 'move' AS src
    FROM stock_moves
    WHERE sku_id IS NOT NULL
)
SELECT src, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM all_events
GROUP BY src
ORDER BY src
