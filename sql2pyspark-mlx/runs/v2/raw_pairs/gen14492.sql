SELECT id, unit_price
FROM order_items
WHERE (order_id = 28 AND unit_price <= 180) OR product_id IN (32, 39)
ORDER BY id DESC;
