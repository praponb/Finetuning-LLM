WITH stacked AS (
    -- Stack the branches
    SELECT sku_id AS entity_id, moved_at AS happened_at, unit_price
    FROM stock_moves
    WHERE MONTH(moved_at) IN (1, 2)
      AND move_type NOT IN ('OUTBOUND', 'TRANSFER')
    UNION
    SELECT sku_id AS entity_id, moved_at AS happened_at, unit_price
    FROM stock_moves
    WHERE sku_id <= 5
      AND operator = 'op_bee'
)
SELECT entity_id, COUNT(*) AS n_rows, SUM(unit_price) AS total_unit_price, MAX(happened_at) AS latest_happened_at
FROM stacked
GROUP BY entity_id
ORDER BY entity_id
