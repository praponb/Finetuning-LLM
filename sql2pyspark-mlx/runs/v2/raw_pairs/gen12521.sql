SELECT s.id, s.end_date
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE city IN ('Berlin', 'Chiang Mai'))
