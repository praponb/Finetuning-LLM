SELECT o.id, o.amount, o.channel
FROM orders o
WHERE o.customer_id NOT IN (SELECT id FROM customers WHERE UPPER(email) LIKE '%ENR%');
