SELECT c.age, c.city, s.customer_id, s.plan, s.status FROM customers c LEFT JOIN subscriptions s ON c.id = s.customer_id;
