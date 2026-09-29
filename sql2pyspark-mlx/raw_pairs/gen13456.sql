SELECT * FROM (SELECT city, country, rating FROM suppliers WHERE rating BETWEEN 2.6 AND 2.8) PIVOT (COUNT(rating) FOR country IN ('SG', 'UK', 'DE')) ORDER BY city
