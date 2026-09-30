WITH stacked AS (
    -- Stack the branches
    SELECT qty, unit_price, 'move' AS origin
    FROM stock_moves
    WHERE sku_id IS NOT NULL
    UNION
    SELECT id AS qty, weight_kg AS unit_price, 'sku' AS origin
    FROM skus
)
SELECT origin, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM stacked
GROUP BY origin
ORDER BY origin
