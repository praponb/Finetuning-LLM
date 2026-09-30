SELECT s.id, s.monthly_fee, s.plan
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE LOWER(TRIM(email)) = 'henry@startup.io');
