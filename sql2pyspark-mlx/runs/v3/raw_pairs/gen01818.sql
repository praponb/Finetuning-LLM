SELECT city, name, ROUND(age / credit_limit, 2) age_per_credit_limit FROM customers WHERE segment IN ('Corporate', 'SMB') ORDER BY age_per_credit_limit;
