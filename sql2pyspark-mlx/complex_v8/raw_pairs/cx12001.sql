WITH stacked AS (
    -- Stack the branches
    SELECT sku_code, weight_kg
    FROM skus
    WHERE category NOT IN ('Electronics', 'Beauty')
      AND ARRAY_CONTAINS(labels, 'bulky')
    UNION ALL
    SELECT sku_code, weight_kg
    FROM skus
)
SELECT sku_code, COUNT(*) AS n_rows, SUM(weight_kg) AS total_weight_kg
FROM stacked
GROUP BY sku_code
ORDER BY sku_code
