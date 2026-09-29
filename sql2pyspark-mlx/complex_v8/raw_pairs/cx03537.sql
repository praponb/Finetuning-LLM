WITH combined AS (
    -- Stack the branches
    SELECT id AS entity_id, sku_id
    FROM stock_moves
    UNION ALL
    SELECT qty AS entity_id, warehouse_id AS sku_id
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-24 12:00:00'
      AND unit_price < 337.6
)
SELECT entity_id, COUNT(*) AS n_rows, SUM(sku_id) AS total_sku_id
FROM combined
GROUP BY entity_id
ORDER BY entity_id
