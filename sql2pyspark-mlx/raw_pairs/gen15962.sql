WITH ProductTotals AS (
    SELECT product_id, COUNT(DISTINCT revenue) AS unique_products, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM store_sales
    WHERE revenue > 2300
    GROUP BY product_id
)
SELECT
    p.id,
    p.brand,
    p.category,
    P.unique_products,
    P.avg_revenue
FROM products p
LEFT JOIN ProductTotals P ON p.id = P.product_id
WHERE P.avg_revenue > 21
ORDER BY unique_products DESC
