SELECT id, discount
FROM orders
WHERE discount <= (SELECT MIN(discount) FROM orders)
ORDER BY discount DESC;
