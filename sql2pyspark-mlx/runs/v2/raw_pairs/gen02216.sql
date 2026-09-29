SELECT * FROM (SELECT city, country, rating FROM suppliers WHERE rating < 4.2)
PIVOT (MAX(rating) FOR country IN ('SG', 'DE', 'US'))
ORDER BY city
