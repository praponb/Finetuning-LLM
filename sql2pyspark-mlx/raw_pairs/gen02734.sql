SELECT c.is_active, COUNT(DISTINCT s.monthly_fee) AS unique_customers FROM subscriptions s JOIN customers c ON c.id = s.customer_id GROUP BY c.is_active;
