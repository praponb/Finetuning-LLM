SELECT id, order_id, unit_price
FROM order_items
WHERE unit_price < 200
ORDER BY id, order_id DESC;
