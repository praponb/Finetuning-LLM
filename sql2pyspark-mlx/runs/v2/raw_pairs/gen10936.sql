SELECT id, country FROM sessions WHERE customer_id IS NULL EXCEPT SELECT id, country FROM sessions WHERE customer_id = 33 ORDER BY country
