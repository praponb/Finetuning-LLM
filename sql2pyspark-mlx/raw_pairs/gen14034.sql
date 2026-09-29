SELECT s.id, s.monthly_fee, c.country, c.age, c.is_active FROM subscriptions s JOIN customers c ON s.customer_id = c.id WHERE c.credit_limit >= 8700;
