SELECT email, ROUND(credit_limit * 1.07, 2) AS credit_limit_scaled FROM customers WHERE LOWER(TRIM(name)) = 'carol diaz' OR credit_limit < 11000;
