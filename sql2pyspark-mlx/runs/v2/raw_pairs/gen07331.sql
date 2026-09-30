SELECT s.id, s.status, s.end_date
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE country IN ('TH', 'US'))
