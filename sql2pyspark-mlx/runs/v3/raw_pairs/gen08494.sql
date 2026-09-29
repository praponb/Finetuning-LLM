SELECT
    p.category,
    COUNT(ss.id) AS store_sale_count,
    MAX(ss.revenue) AS max_revenue,
    MIN(ss.sale_date) AS first_sale_date
FROM products p
LEFT OUTER JOIN store_sales ss ON p.id = ss.product_id
WHERE p.cost <= 400
GROUP BY p.category
ORDER BY max_revenue DESC, category
LIMIT 20
