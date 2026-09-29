WITH all_events AS (
    -- Stack the branches
    SELECT
        'warehouse' AS source,
        id AS ref_id,
        city,
        capacity
    FROM warehouses
    UNION ALL
    SELECT
        'move' AS source,
        sku_id AS ref_id,
        operator AS city,
        warehouse_id AS capacity
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
      AND unit_price < 393.3
)
SELECT source, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM all_events
GROUP BY source
ORDER BY source
