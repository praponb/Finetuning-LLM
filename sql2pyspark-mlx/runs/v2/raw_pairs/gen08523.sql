SELECT oi.id, oi.unit_price, oi.product_id FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE store_id IN (4, 15, 21, 27)) ORDER BY oi.unit_price
