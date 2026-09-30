SELECT *
FROM order_items
WHERE order_id IN (15, 23, 40) OR product_id IN (26, 30, 33);
