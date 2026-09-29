SELECT c.city, c.email, s.start_date, s.plan, s.monthly_fee FROM customers c JOIN subscriptions s ON s.customer_id = c.id ORDER BY s.start_date ASC;
