SELECT * FROM (SELECT country, segment, age FROM customers) src PIVOT (AVG(age) AS amount, COUNT(1) AS item_count FOR segment IN ('Corporate', 'SMB', 'Consumer'))
