SELECT c.name, s.plan, s.customer_id
FROM customers c
LEFT OUTER JOIN subscriptions s ON s.customer_id = c.id
WHERE s.customer_id IS NOT NULL
ORDER BY plan ASC, s.customer_id DESC
