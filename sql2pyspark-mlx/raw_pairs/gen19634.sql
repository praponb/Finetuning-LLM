SELECT c.credit_limit, s.monthly_fee, s.end_date
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
ORDER BY credit_limit DESC;
