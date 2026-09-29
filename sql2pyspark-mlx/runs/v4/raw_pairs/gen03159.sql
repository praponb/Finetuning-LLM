SELECT
    p.id,
    p.stock_qty,
    p.price,
    t.avg_revenue
FROM products p
LEFT OUTER JOIN (SELECT product_id, AVG(revenue) avg_revenue
    FROM store_sales
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.avg_revenue >= 3000
