SELECT id, unit_price, quantity
FROM order_items
WHERE unit_price > (SELECT MAX(unit_price) FROM order_items WHERE order_id IN (30, 34))
ORDER BY unit_price DESC;
