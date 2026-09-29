SELECT
    p.category,
    p.brand,
    MIN(ss.revenue) AS min_revenue,
    SUM(ss.revenue) AS total_revenue
FROM store_sales ss
FULL OUTER JOIN stores s ON s.id = ss.store_id
LEFT JOIN products p ON ss.product_id = p.id
GROUP BY p.category, p.brand
HAVING MIN(ss.revenue) > 2300
