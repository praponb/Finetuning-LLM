SELECT
    id,
    age,
    email,
    is_active
FROM customers
WHERE age = (SELECT MIN(age) FROM customers WHERE credit_limit BETWEEN 8600 AND 13600)
ORDER BY email, id
LIMIT 5;
