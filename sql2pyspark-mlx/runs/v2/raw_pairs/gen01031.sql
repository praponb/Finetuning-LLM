WITH product_stats AS (
    SELECT product_id, MAX(sale_date) latest_sale_date
    FROM store_sales
    GROUP BY product_id
)
SELECT p.id, p.supplier_id, ps.latest_sale_date
FROM products p
LEFT OUTER JOIN product_stats ps ON p.id = ps.product_id
