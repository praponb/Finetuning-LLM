SELECT id, amount, status
FROM orders
WHERE amount < (SELECT MIN(amount) FROM orders);
