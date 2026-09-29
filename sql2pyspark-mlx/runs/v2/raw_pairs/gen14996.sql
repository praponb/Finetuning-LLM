SELECT * FROM (SELECT city, segment, age FROM customers) PIVOT (COUNT(age) FOR segment IN ('SMB', 'Consumer', 'Corporate'));
