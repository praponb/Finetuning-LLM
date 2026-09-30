WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-13 12:00:00'
),
combined AS (
    -- Stack the branches
    SELECT 'move' AS record_type, qty AS entity_id, id
    FROM cleaned_moves
    UNION
    SELECT 'move_2' AS record_type, id, warehouse_id
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-02 10:00:00'
      AND qty >= 198
)
SELECT record_type, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM combined
GROUP BY record_type
ORDER BY record_type
