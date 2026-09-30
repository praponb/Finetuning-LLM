SELECT id, status, amount
FROM orders
WHERE amount > 1570
ORDER BY status, id DESC
LIMIT 3
