SELECT * FROM (SELECT city, country, rating FROM suppliers)
PIVOT (SUM(rating) FOR country IN ('DE', 'US', 'UK', 'SG'))
ORDER BY city;
