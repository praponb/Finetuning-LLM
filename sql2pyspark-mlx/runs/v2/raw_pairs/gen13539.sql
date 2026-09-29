SELECT * FROM (SELECT city, country, rating FROM suppliers WHERE rating BETWEEN 3.4 AND 3.8)
PIVOT (COUNT(rating) FOR country IN ('US', 'DE', 'JP'))
