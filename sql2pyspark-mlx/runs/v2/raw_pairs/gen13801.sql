SELECT id, credit_limit
FROM customers
WHERE name IS NOT NULL AND country IN ('JP', 'TH', 'UK') AND email NOT LIKE '%@yahoo.com'
ORDER BY id
LIMIT 50;
