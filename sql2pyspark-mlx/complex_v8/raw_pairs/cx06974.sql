WITH stacked AS (
    -- Stack the branches
    SELECT operator, move_type
    FROM stock_moves
    UNION ALL
    SELECT category AS operator, sku_code AS move_type
    FROM skus
    WHERE UPPER(TRIM(sku_code)) LIKE 'A%'
)
SELECT operator, COUNT(*) AS n_rows
FROM stacked
GROUP BY operator
ORDER BY operator
