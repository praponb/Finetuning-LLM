SELECT s.id, s.status, s.end_date FROM subscriptions s WHERE s.monthly_fee >= 170 AND s.customer_id NOT IN (SELECT id FROM customers WHERE city IN ('Tokyo', 'Chiang Mai', 'Singapore'))
