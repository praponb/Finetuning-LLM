SELECT quantity
FROM order_items
WHERE quantity BETWEEN 3 AND 7
UNION ALL
SELECT quantity
FROM order_items
WHERE quantity = 6
