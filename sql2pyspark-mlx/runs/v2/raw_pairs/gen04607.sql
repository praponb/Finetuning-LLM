SELECT c.id, c.age FROM customers c WHERE EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.monthly_fee < 100);
