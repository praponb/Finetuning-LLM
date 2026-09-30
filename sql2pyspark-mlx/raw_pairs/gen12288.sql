WITH ProductStats AS (
    SELECT product_id, ROUND(AVG(units), 2) AS avg_units
    FROM store_sales
    GROUP BY product_id
)
SELECT p.brand, P.avg_units
FROM products p
LEFT OUTER JOIN ProductStats P ON p.id = P.product_id
WHERE P.avg_units >= 45
ORDER BY avg_units DESC
