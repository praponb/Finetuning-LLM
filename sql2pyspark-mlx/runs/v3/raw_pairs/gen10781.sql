SELECT id, age, email, CONCAT_WS('/', name, segment) AS name_segment_key FROM customers WHERE name IS NOT NULL
