SELECT id, credit_limit, country, UPPER(name) AS name_upper FROM customers WHERE segment IS NULL ORDER BY credit_limit DESC
