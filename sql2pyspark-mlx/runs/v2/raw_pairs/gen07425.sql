WITH product_summary AS (
    SELECT product_id, MAX(quantity) AS max_quantity
    FROM order_items
    WHERE unit_price <= 130
    GROUP BY product_id
)
SELECT p.supplier_id, p.product_name, ps.max_quantity
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.max_quantity >= 3.2
ORDER BY supplier_id ASC;
