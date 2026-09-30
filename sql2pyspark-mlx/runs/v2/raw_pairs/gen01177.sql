SELECT id, segment
FROM customers
WHERE signup_date BETWEEN '2023-12-01' AND '2025-02-01'
UNION
SELECT id, segment
FROM customers
WHERE email IS NULL;
