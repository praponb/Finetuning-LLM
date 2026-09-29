SELECT p.method, o.channel, c.age
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
LEFT JOIN customers c ON c.id = o.customer_id
ORDER BY age;
