SELECT
    o.id,
    o.customer_id,
    o.channel,
    c.age,
    c.city,
    s.store_name
FROM orders o
RIGHT JOIN customers c ON c.id = o.customer_id
INNER JOIN stores s ON s.id = o.store_id
ORDER BY o.channel
