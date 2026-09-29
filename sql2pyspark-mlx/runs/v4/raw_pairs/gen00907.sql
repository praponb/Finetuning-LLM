SELECT s.id, s.country, s.started_at FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE signup_date BETWEEN '2024-05-02' AND '2024-09-01') ORDER BY id
