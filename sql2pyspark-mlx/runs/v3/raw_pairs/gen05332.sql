SELECT s.id, s.monthly_fee, s.end_date
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE LOWER(name) LIKE '%ob %')
