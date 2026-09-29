SELECT * FROM (SELECT city, country, rating FROM suppliers)
PIVOT (SUM(rating) FOR country IN ('SG', 'DE', 'TH', 'UK'))
ORDER BY city;
