SELECT * FROM (SELECT city, segment, age FROM customers) PIVOT (AVG(age) FOR segment IN ('Corporate', 'SMB', 'Consumer'));
