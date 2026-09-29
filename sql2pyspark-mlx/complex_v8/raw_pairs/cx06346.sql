WITH all_events AS (
    -- Stack the branches
    SELECT sku_id, move_type
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'TRANSFER', 'OUTBOUND')
    UNION
    SELECT id, sku_code
    FROM skus
    WHERE weight_kg < 20.9
)
SELECT move_type, COUNT(*) AS n_rows, SUM(sku_id) AS total_sku_id
FROM all_events
GROUP BY move_type
ORDER BY move_type
