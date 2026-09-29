SELECT id, unit_price, order_id
FROM order_items
WHERE unit_price < 120 AND unit_price < 140
ORDER BY id DESC
LIMIT 1
