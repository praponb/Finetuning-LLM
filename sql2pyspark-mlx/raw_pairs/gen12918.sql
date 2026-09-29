SELECT * FROM (SELECT city, country, rating FROM suppliers) PIVOT (COUNT(rating) FOR country IN ('SG', 'TH', 'US'))
