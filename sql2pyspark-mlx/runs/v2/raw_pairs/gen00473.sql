SELECT s.id, s.browser FROM sessions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE signup_date >= '2024-07-04');
