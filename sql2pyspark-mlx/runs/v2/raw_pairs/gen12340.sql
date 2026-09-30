SELECT id, segment, signup_date, credit_limit, LEAD(credit_limit, 2) OVER (PARTITION BY segment ORDER BY signup_date, id) AS next_credit_limit FROM customers WHERE credit_limit IS NOT NULL;
