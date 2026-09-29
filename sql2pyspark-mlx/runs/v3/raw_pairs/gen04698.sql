SELECT quantity
FROM order_items
WHERE quantity <> 7
UNION ALL
SELECT quantity
FROM order_items
WHERE order_id = 37
ORDER BY quantity;
