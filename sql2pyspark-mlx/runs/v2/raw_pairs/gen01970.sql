SELECT id, credit_limit, name FROM customers WHERE age BETWEEN 40 AND 67 OR MONTH(signup_date) IN (7, 8, 9)
