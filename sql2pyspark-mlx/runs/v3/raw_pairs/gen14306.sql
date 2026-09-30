SELECT
    p.id,
    p.category,
    t.total_revenue,
    t.min_units
FROM products p
LEFT JOIN (SELECT product_id, SUM(revenue) AS total_revenue, MIN(units) AS min_units
    FROM store_sales
    WHERE store_id IN (2, 3, 5, 6)
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.total_revenue >= 11000
ORDER BY t.total_revenue
