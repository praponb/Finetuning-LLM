SELECT credit_limit, signup_date, email, credit_limit - age AS credit_limit_minus_age FROM customers WHERE credit_limit <= 4700 AND (age != 56 OR segment != 'Corporate') ORDER BY credit_limit DESC
