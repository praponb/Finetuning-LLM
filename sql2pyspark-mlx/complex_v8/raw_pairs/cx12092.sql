WITH stacked AS (
    -- Stack the branches
    SELECT
        'move' AS record_type,
        sku_id,
        qty AS key_id,
        unit_price AS metric
    FROM stock_moves
    UNION ALL
    SELECT
        'move_2' AS record_type,
        id,
        warehouse_id,
        unit_price
    FROM stock_moves
)
SELECT record_type, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM stacked
GROUP BY record_type
ORDER BY record_type
