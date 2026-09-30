SELECT id, name, signup_date, credit_limit, ROUND(age / credit_limit, 2) AS age_per_credit_limit FROM customers WHERE segment IN ('Consumer', 'SMB', 'Corporate');
