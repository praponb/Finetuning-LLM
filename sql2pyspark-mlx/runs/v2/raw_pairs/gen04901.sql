SELECT s.id, s.status, s.plan, s.monthly_fee FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE LENGTH(name) >= 14) ORDER BY id DESC;
