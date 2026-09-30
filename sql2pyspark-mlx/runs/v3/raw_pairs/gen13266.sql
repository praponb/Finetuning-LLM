SELECT quantity
FROM order_items
WHERE order_id IN (7, 16, 18)
UNION ALL
SELECT quantity
FROM order_items
WHERE quantity < 6
ORDER BY quantity
