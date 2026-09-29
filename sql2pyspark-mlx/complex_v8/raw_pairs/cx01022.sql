WITH stacked AS (
    -- Stack the branches
    SELECT operator AS name, move_type AS category, unit_price
    FROM stock_moves
    UNION
    SELECT sku_code, category, weight_kg
    FROM skus
    WHERE ARRAY_CONTAINS(labels, 'cold')
      AND weight_kg <= 18.5
)
SELECT name, COUNT(*) AS n_rows, SUM(unit_price) AS total_unit_price
FROM stacked
GROUP BY name
ORDER BY name
