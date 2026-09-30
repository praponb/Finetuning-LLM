SELECT * FROM (SELECT city, country, rating FROM suppliers)
PIVOT (MAX(rating) FOR country IN ('JP', 'SG', 'UK'))
