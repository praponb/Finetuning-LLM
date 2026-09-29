WITH stacked AS (
    -- Stack the branches
    SELECT
        operator AS category,
        move_type AS label,
        unit_price,
        'move' AS source
    FROM stock_moves
    UNION ALL
    SELECT
        move_type,
        operator,
        unit_price,
        'move_2' AS source
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-31 09:00:00'
)
SELECT source, COUNT(*) AS n_rows, SUM(unit_price) AS total_unit_price
FROM stacked
GROUP BY source
ORDER BY source
