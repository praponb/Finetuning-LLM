SELECT
    id,
    age,
    name,
    credit_limit
FROM customers
WHERE age >= (SELECT AVG(age) FROM customers)
ORDER BY id DESC;
