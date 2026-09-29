SELECT s.id, s.start_date, s.plan, s.status FROM subscriptions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE email LIKE '%@outlook.com')
