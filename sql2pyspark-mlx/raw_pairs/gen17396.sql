SELECT id, product_id
FROM order_items
WHERE product_id IN (3, 15) AND (unit_price >= 280 OR unit_price > 250);
