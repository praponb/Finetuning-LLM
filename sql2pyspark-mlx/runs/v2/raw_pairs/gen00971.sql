SELECT * FROM (SELECT country, city, credit_limit FROM customers WHERE is_active = TRUE)
PIVOT (AVG(credit_limit) FOR city IN ('Berlin', 'Bangkok', 'Tokyo'))
ORDER BY country
