SELECT * FROM (SELECT city, country, rating FROM suppliers WHERE city IN ('Berlin', 'Chiang Mai', 'Singapore'))
PIVOT (COUNT(rating) FOR country IN ('DE', 'UK', 'US'))
