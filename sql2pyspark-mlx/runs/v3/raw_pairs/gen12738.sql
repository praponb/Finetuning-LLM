SELECT id, discount
FROM orders
WHERE discount <= (SELECT MIN(discount) FROM orders)
ORDER BY id DESC;
