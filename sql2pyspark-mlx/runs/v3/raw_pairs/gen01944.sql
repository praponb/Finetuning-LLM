SELECT
    p.brand,
    p.product_name,
    t.revenue_count,
    t.total_revenue
FROM products p
LEFT OUTER JOIN (SELECT product_id, COUNT(revenue) AS revenue_count, SUM(revenue) AS total_revenue
    FROM store_sales
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY t.total_revenue DESC
