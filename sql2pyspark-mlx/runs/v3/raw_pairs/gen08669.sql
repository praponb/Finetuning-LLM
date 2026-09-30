WITH StoreSalesByProduct AS (
    SELECT product_id, AVG(revenue) AS avg_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT p.id, p.brand, S.avg_revenue
FROM products p
LEFT OUTER JOIN StoreSalesByProduct S ON p.id = S.product_id
WHERE S.avg_revenue >= 1600
ORDER BY S.avg_revenue, id DESC
