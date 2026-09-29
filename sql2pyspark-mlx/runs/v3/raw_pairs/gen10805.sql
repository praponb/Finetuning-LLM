SELECT s.id, s.status, s.customer_id, s.plan FROM subscriptions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE email LIKE '%@company.co')
