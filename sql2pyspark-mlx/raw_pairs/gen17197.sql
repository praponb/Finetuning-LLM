SELECT MIN(credit_limit) AS min_credit_limit, COUNT(*) AS num_customers FROM customers ORDER BY num_customers DESC;
