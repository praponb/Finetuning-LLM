SELECT s.id, s.country, s.device
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE signup_date BETWEEN '2024-04-01' AND '2025-07-02');
