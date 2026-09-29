WITH unioned AS (
    -- Stack the branches
    SELECT sku_id, unit_price
    FROM stock_moves
    WHERE qty < 370
    UNION
    SELECT sku_id, unit_price
    FROM stock_moves
)
SELECT sku_id, COUNT(*) AS n_rows, SUM(unit_price) AS total_unit_price
FROM unioned
GROUP BY sku_id
ORDER BY sku_id
