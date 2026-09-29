SELECT o.discount, c.segment
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
WHERE c.city IS NOT NULL;
