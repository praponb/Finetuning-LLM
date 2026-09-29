SELECT id, customer_id, country FROM sessions WHERE browser IS NULL UNION SELECT id, customer_id, country FROM sessions WHERE browser IS NULL ORDER BY id;
