SELECT country, segment, MIN(credit_limit) AS min_credit_limit FROM customers GROUP BY country, segment
