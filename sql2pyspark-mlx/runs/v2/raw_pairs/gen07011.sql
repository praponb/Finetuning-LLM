SELECT id, order_id, quantity, ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id), 3) AS quantity_share FROM order_items WHERE order_id IN (27, 28, 36) ORDER BY id DESC;
