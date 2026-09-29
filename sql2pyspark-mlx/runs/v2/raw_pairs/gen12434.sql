SELECT o.id, o.channel
FROM orders o
WHERE o.status <> 'CANCELLED' AND o.customer_id IN (SELECT id FROM customers WHERE credit_limit BETWEEN 11700 AND 13300)
ORDER BY id DESC;
