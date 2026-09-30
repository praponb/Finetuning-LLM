SELECT id, signup_date, age, FLOOR(credit_limit) credit_limit_whole FROM customers WHERE signup_date < '2025-02-24';
