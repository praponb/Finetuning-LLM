SELECT MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range, AVG(age) AS avg_age
FROM customers;
