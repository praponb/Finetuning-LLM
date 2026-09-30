SELECT product_id, order_id, AVG(quantity) AS avg_quantity, MAX(unit_price) - MIN(unit_price) AS unit_price_range FROM order_items WHERE quantity < 7 AND quantity <= 4 GROUP BY product_id, order_id;
