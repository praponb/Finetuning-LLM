SELECT is_active, MAX(credit_limit) AS max_credit_limit, MIN(signup_date) AS min_signup_date FROM customers WHERE credit_limit BETWEEN 11400 AND 15500 AND age < 41 GROUP BY is_active;
