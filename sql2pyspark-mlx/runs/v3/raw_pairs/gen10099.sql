SELECT id, signup_date, credit_limit, SUM(credit_limit) OVER (ORDER BY signup_date, id) AS running_credit_limit FROM customers;
