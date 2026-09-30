SELECT id, discount, customer_id
FROM orders
WHERE status <> 'RETURNED';
