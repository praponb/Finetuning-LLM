SELECT id, product_id
FROM order_items
WHERE unit_price < 320
EXCEPT
SELECT id, product_id
FROM order_items
WHERE order_id IN (5, 10, 20, 29);
