SELECT s.id, s.started_at, s.country, s.customer_id FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE signup_date > '2025-07-07')
