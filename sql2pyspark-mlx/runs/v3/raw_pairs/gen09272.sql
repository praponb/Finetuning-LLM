SELECT s.id, s.started_at FROM sessions s WHERE s.customer_id IN (SELECT id FROM customers WHERE signup_date > '2025-05-10');
