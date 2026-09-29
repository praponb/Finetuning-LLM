SELECT s.id, s.browser, s.customer_id
FROM sessions s
WHERE s.browser IS NOT NULL AND s.customer_id IN (SELECT id FROM customers WHERE segment != 'Corporate')
ORDER BY s.id DESC
LIMIT 20;
