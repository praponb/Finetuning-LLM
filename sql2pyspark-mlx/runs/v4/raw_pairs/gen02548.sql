SELECT * FROM (SELECT country, segment, age FROM customers WHERE segment != 'Consumer')
PIVOT (MAX(age) FOR segment IN ('Corporate', 'SMB'));
