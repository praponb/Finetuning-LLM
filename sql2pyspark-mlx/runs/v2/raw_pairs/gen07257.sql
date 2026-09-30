SELECT id, order_id, quantity
FROM order_items
WHERE unit_price < 270 AND (order_id IN (21, 24, 30, 40) OR quantity IS NOT NULL)
ORDER BY order_id DESC, 3 DESC, id
LIMIT 5
