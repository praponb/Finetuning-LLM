SELECT city
FROM customers
WHERE age BETWEEN 47 AND 54
UNION
SELECT city
FROM customers
WHERE signup_date <= '2024-05-01'
