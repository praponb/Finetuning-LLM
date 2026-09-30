SELECT SUM(age) AS total_age, COUNT(credit_limit) AS credit_limit_count
FROM customers
WHERE city IN ('Berlin', 'Tokyo', 'London') AND (signup_date BETWEEN '2024-08-06' AND '2025-05-08' OR credit_limit >= 6200);
