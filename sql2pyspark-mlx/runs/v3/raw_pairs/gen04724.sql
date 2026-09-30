SELECT s.id, s.device
FROM sessions s
WHERE s.browser IN ('Safari', 'Edge', 'Chrome') AND s.customer_id IN (SELECT id FROM customers WHERE YEAR(signup_date) >= 2023)
ORDER BY device DESC, id
LIMIT 3;
