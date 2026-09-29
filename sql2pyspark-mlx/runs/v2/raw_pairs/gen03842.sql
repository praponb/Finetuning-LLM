SELECT id, country, credit_limit, AVG(credit_limit) OVER (PARTITION BY country) AS country_avg_credit_limit FROM customers;
