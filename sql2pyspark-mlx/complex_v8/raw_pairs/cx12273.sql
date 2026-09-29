WITH stacked AS (
    -- Stack the branches
    SELECT 'warehouse' AS source, capacity AS entity_id, id AS qty
    FROM warehouses
    WHERE wh_code IS NOT NULL
      AND city = 'Bangkok'
    UNION ALL
    SELECT 'move' AS source, sku_id, qty
    FROM stock_moves
)
SELECT source, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM stacked
GROUP BY source
ORDER BY source
