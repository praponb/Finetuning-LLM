SELECT id, unit_price
FROM order_items
WHERE quantity < 7 AND quantity != 3 AND quantity BETWEEN 4 AND 8
