SELECT * FROM (SELECT city, country, rating FROM suppliers)
PIVOT (COUNT(rating) FOR country IN ('DE', 'US', 'JP', 'UK'))
ORDER BY city
