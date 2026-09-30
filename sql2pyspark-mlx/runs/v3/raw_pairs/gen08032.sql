SELECT * FROM (SELECT country, city, age FROM customers WHERE country IN ('US', 'TH', 'UK'))
PIVOT (COUNT(age) FOR city IN ('Berlin', 'Singapore', 'New York', 'Tokyo'))
