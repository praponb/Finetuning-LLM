SELECT id, signup_date, is_active, age * credit_limit AS age_times_credit_limit, credit_limit + age AS credit_limit_plus_age FROM customers WHERE city <> 'Bangkok'
