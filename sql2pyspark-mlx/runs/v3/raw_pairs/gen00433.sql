SELECT c.name, c.city, s.plan, s.status, s.start_date FROM customers c LEFT JOIN subscriptions s ON c.id = s.customer_id;
