SELECT id, age, segment, signup_date, ROUND(credit_limit * 1.1, 2) AS credit_limit_adjusted FROM customers WHERE age > 57 AND LENGTH(email) > 10 AND credit_limit > 7700
