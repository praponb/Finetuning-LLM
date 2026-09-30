WITH product_summary AS (
    SELECT product_id, COUNT(*) AS num_store_sales, SUM(revenue) AS total_revenue
    FROM store_sales
    WHERE store_id IN (2, 3, 5, 6)
    GROUP BY product_id
)
SELECT
    p.product_name,
    p.category,
    ps.num_store_sales,
    ps.total_revenue
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.total_revenue > 9000;
