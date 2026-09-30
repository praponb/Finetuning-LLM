SELECT customer_id
FROM orders
WHERE shipping_fee < 12 OR status IN ('RETURNED', 'COMPLETED', 'PENDING')
ORDER BY customer_id DESC;
