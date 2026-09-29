SELECT id, product_id, quantity, ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 3) AS quantity_share FROM order_items WHERE order_id = 5;
