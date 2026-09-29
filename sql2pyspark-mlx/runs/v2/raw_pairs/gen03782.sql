SELECT c.id, c.city
FROM customers c
WHERE c.signup_date < '2024-02-01' AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.plan = 'standard')
ORDER BY city ASC
