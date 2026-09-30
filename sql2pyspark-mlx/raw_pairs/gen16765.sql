SELECT s.id, s.status FROM subscriptions s WHERE s.end_date >= '2023-07-01' AND s.customer_id IN (SELECT id FROM customers WHERE segment = 'Corporate') ORDER BY status
