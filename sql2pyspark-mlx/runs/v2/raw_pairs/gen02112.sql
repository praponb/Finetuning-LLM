SELECT id, unit_price, order_id
FROM order_items
WHERE unit_price > (SELECT AVG(unit_price) FROM order_items)
ORDER BY id
