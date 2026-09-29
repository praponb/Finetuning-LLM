SELECT
    o.id,
    o.store_id,
    c.name,
    c.segment,
    c.city
FROM orders o
RIGHT JOIN customers c ON c.id = o.customer_id
ORDER BY store_id DESC;
