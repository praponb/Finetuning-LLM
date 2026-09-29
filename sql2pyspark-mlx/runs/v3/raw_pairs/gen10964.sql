SELECT id, country, CASE WHEN country = 'SG' THEN 1 ELSE 0 END is_sg
FROM customers
WHERE age >= 41;
