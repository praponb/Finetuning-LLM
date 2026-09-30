SELECT name, email, UPPER(TRIM(name)) AS name_normalized, LEAST(credit_limit, age) AS least_credit_limit_age FROM customers WHERE city IN ('Bangkok', 'Singapore', 'Berlin')
