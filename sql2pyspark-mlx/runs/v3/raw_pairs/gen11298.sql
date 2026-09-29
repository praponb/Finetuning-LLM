SELECT s.id, s.end_date, s.plan, s.status FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE country = 'US') ORDER BY s.status DESC, id LIMIT 10;
