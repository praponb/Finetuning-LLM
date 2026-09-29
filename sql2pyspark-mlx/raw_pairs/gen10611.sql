SELECT id, signup_date, credit_limit, SUM(credit_limit) OVER (PARTITION BY MONTH(signup_date) ORDER BY signup_date, id) AS running_credit_limit FROM customers;
