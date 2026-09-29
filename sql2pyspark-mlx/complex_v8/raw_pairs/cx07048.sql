WITH stacked AS (
    -- Stack the branches
    SELECT sku_id AS key_id, operator
    FROM stock_moves
    UNION ALL
    SELECT qty AS key_id, move_type AS operator
    FROM stock_moves
    UNION ALL
    SELECT id AS key_id, wh_code AS operator
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%x1%'
      AND capacity > 4912
)
SELECT operator, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM stacked
GROUP BY operator
ORDER BY operator
