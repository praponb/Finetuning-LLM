SELECT s.start_date, s.monthly_fee, c.age FROM subscriptions s JOIN customers c ON c.id = s.customer_id;
