SELECT
    p.id,
    p.launch_date,
    p.stock_qty,
    t.avg_revenue
FROM products p
INNER JOIN (SELECT product_id, AVG(revenue) AS avg_revenue
    FROM store_sales
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.avg_revenue >= 3200
ORDER BY launch_date DESC
