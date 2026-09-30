SELECT * FROM (SELECT country, city, credit_limit FROM customers)
PIVOT (AVG(credit_limit) FOR city IN ('Bangkok', 'Chiang Mai'))
ORDER BY country;
