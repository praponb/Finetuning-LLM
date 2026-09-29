SELECT o.id, o.store_id, o.channel
FROM orders o
WHERE o.customer_id IN (SELECT id FROM customers WHERE MONTH(signup_date) IN (5, 10, 11))
ORDER BY 3;
