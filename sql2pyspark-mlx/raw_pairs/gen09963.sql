SELECT id, unit_price, quantity
FROM order_items
WHERE unit_price > (SELECT MAX(unit_price) FROM order_items WHERE product_id IS NULL)
ORDER BY unit_price DESC;
