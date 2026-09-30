SELECT order_id
FROM order_items
WHERE unit_price <= 190
UNION
SELECT order_id
FROM order_items
WHERE product_id IS NOT NULL;
