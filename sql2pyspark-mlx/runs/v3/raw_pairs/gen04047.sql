SELECT credit_limit, signup_date, city, CAST(credit_limit AS INT) AS credit_limit_int FROM customers WHERE country IN ('US', 'DE', 'UK') ORDER BY credit_limit DESC, city DESC;
