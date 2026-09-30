SELECT quantity
FROM order_items
WHERE order_id IN (7, 28)
UNION ALL
SELECT quantity
FROM order_items
WHERE unit_price < 280
