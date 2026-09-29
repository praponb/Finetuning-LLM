SELECT * FROM (SELECT city, country, rating FROM suppliers)
PIVOT (MAX(rating) FOR country IN ('SG', 'UK', 'TH'))
ORDER BY city;
