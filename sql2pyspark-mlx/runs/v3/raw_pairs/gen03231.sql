WITH product_summary AS (
    SELECT product_id, COUNT(revenue) AS revenue_count
    FROM store_sales
    GROUP BY product_id
)
SELECT p.stock_qty, p.price, ps.revenue_count
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.revenue_count > 6
ORDER BY stock_qty ASC, price DESC
