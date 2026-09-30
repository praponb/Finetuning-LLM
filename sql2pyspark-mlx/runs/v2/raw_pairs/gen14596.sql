WITH product_stats AS (
    SELECT product_id, MAX(revenue) - MIN(revenue) AS revenue_range, MIN(sale_date) AS first_sale_date
    FROM store_sales
    GROUP BY product_id
)
SELECT
    p.id,
    p.supplier_id,
    ps.revenue_range,
    ps.first_sale_date
FROM products p
JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.revenue_range >= 46
ORDER BY p.supplier_id, revenue_range DESC, id
LIMIT 50;
