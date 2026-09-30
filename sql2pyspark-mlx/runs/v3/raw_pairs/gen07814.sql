SELECT id, amount, shipping_fee
FROM orders
WHERE amount > (SELECT AVG(amount) FROM orders)
ORDER BY 2
