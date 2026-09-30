SELECT s.id, s.plan, s.start_date
FROM subscriptions s
WHERE s.plan = 'standard' AND s.customer_id IN (SELECT id FROM customers WHERE country = 'TH');
