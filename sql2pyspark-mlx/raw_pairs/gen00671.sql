SELECT order_id, MIN(quantity) AS min_quantity, MIN(unit_price) AS min_unit_price
FROM order_items
WHERE product_id IN (15, 23) AND order_id = 4 AND order_id IN (7, 8, 21)
GROUP BY order_id;
