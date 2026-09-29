SELECT * FROM (SELECT segment, country, COUNT(credit_limit) AS credit_limit_count FROM customers WHERE MONTH(signup_date) IN (4, 8, 12) GROUP BY segment, country) AS t WHERE credit_limit_count > 3
