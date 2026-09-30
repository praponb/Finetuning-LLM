SELECT id, unit_price, product_id
FROM order_items
WHERE unit_price >= (SELECT AVG(unit_price) FROM order_items WHERE product_id IS NOT NULL);
