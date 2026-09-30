SELECT s.end_date, s.start_date, s.plan, c.age, c.country FROM subscriptions s INNER JOIN customers c ON c.id = s.customer_id;
