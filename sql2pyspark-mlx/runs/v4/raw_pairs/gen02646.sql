SELECT id, quantity, order_id
FROM order_items
WHERE quantity >= (SELECT AVG(quantity) FROM order_items WHERE unit_price <= 130)
ORDER BY order_id DESC, id
LIMIT 5;
