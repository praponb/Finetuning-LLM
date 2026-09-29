SELECT id, country, credit_limit, MIN(credit_limit) OVER (PARTITION BY country) AS country_min_credit_limit FROM customers
