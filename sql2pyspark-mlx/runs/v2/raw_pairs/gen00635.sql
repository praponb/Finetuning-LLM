SELECT id, discount
FROM orders
WHERE discount > (SELECT AVG(discount) FROM orders);
