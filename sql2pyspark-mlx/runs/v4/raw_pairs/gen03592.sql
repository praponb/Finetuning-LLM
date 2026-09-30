SELECT quantity, ROUND(quantity * 1.1, 2) AS adjusted_quantity
FROM order_items
WHERE quantity < 7 AND order_id IN (16, 23, 30);
