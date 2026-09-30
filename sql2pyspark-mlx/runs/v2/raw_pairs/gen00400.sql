SELECT id, country
FROM customers
WHERE LENGTH(email) >= 14
UNION
SELECT id, country
FROM customers
WHERE signup_date BETWEEN '2024-09-11' AND '2025-03-01'
ORDER BY id;
