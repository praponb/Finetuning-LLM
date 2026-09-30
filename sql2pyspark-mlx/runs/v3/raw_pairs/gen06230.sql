SELECT order_id, COUNT(DISTINCT unit_price) AS unique_orders FROM order_items WHERE order_id IN (16, 36, 40) AND unit_price <= 160 GROUP BY order_id ORDER BY unique_orders DESC;
