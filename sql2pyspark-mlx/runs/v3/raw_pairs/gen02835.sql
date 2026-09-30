SELECT product_id
FROM order_items
WHERE unit_price <= 340
UNION ALL
SELECT product_id
FROM order_items
WHERE quantity != 3
