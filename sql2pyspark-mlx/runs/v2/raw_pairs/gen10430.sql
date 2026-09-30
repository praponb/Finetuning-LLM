SELECT id, unit_price
FROM order_items
WHERE order_id IN (2, 29, 31)
ORDER BY id DESC
