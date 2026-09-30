SELECT * FROM (SELECT country, city, credit_limit FROM customers)
PIVOT (COUNT(credit_limit) FOR city IN ('New York', 'Chiang Mai'))
ORDER BY country;
