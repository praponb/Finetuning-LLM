SELECT * FROM (SELECT segment, city, credit_limit FROM customers)
PIVOT (MAX(credit_limit) FOR city IN ('Chiang Mai', 'Bangkok', 'Berlin'))
ORDER BY segment;
