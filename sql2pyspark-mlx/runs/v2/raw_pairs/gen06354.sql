SELECT city, SUM(age) AS total_age, COUNT(credit_limit) AS credit_limit_count, SUM(credit_limit) AS total_credit_limit FROM customers GROUP BY city HAVING COUNT(age) >= 2
