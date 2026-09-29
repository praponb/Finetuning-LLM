WITH unioned AS (
    -- Stack the branches
    SELECT sku_code, weight_kg, category AS name
    FROM skus
    UNION ALL
    SELECT move_type AS sku_code, unit_price AS weight_kg, operator AS name
    FROM stock_moves
    WHERE qty >= 169
      AND warehouse_id <= 2
    UNION ALL
    SELECT move_type AS sku_code, unit_price AS weight_kg, operator AS name
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND sku_id >= 6
)
SELECT sku_code, COUNT(*) AS n_rows, SUM(weight_kg) AS total_weight_kg
FROM unioned
GROUP BY sku_code
ORDER BY sku_code
