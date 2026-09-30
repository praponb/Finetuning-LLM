SELECT product_id, unit_price, quantity
FROM order_items
WHERE order_id IN (2, 6, 10, 40) AND order_id IN (17, 28);
