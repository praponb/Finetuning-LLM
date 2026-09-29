SELECT id, segment, signup_date, credit_limit, LAG(credit_limit, 1) OVER (PARTITION BY segment ORDER BY signup_date, id) AS prev_credit_limit FROM customers
