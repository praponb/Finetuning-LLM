SELECT s.start_date, s.monthly_fee, c.city
FROM subscriptions s
LEFT JOIN customers c ON s.customer_id = c.id;
