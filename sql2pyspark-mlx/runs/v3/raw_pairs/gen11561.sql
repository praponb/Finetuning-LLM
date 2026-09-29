SELECT id, signup_date, segment, age, age - credit_limit AS age_minus_credit_limit FROM customers WHERE UPPER(name) LIKE '%ARO%';
