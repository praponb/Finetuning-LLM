WITH product_totals AS (
    SELECT product_id, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM store_sales
    GROUP BY product_id
)
SELECT p.id, p.launch_date, pt.revenue_range
FROM products p
INNER JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.revenue_range > 55
