SELECT * FROM (SELECT segment, country, age FROM customers)
PIVOT (SUM(age) FOR country IN ('DE', 'JP', 'TH'))
ORDER BY segment;
