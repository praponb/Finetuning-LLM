SELECT id, product_id, quantity - unit_price AS line_total
FROM order_items
WHERE product_id IN (5, 20) AND quantity IS NOT NULL;
