WITH stacked AS (
    -- Stack the branches
    SELECT id AS entity_id, unit_price, 'move' AS origin
    FROM stock_moves
    UNION ALL
    SELECT id, unit_price, 'move_2' AS origin
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'OUTBOUND')
    UNION ALL
    SELECT id, weight_kg, 'sku' AS origin
    FROM skus
)
SELECT origin, COUNT(*) AS n_rows, SUM(unit_price) AS total_unit_price
FROM stacked
GROUP BY origin
ORDER BY origin
