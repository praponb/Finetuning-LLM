SELECT s.id, s.end_date, s.plan, c.email FROM subscriptions s JOIN customers c ON s.customer_id = c.id WHERE s.plan = 'premium'
