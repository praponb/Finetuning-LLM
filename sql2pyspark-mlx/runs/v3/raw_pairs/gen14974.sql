SELECT p.id, p.cost, t.avg_revenue
FROM products p
JOIN (SELECT product_id, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM store_sales
    WHERE store_id IN (1, 3)
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY p.id DESC
