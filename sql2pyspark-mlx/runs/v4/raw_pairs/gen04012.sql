SELECT id, credit_limit, CONCAT_WS('-', country, segment) AS country_segment_key
FROM customers
WHERE email LIKE '%@gmail.com';
