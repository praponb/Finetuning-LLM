WITH product_totals AS (
    SELECT product_id, MAX(sale_date) last_sale_date
    FROM store_sales
    GROUP BY product_id
)
SELECT
    p.id,
    p.cost,
    p.launch_date,
    pt.last_sale_date
FROM products p
JOIN product_totals pt ON p.id = pt.product_id
ORDER BY id DESC
LIMIT 10;
