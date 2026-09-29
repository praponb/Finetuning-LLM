SELECT s.id, s.device, s.customer_id
FROM sessions s
WHERE s.device IN ('tablet', 'desktop', 'mobile') AND s.customer_id IN (SELECT id FROM customers WHERE MONTH(signup_date) IN (1, 4, 5))
ORDER BY device DESC
