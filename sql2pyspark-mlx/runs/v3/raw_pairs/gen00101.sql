SELECT
    s.id,
    s.device,
    s.customer_id,
    s.country
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE signup_date BETWEEN '2024-08-01' AND '2024-08-09');
