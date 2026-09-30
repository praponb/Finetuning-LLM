SELECT * FROM (SELECT country, city, credit_limit FROM customers) PIVOT (MAX(credit_limit) FOR city IN ('Chiang Mai', 'New York', 'Tokyo'));
