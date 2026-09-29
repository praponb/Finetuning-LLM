SELECT * FROM (SELECT city, country, rating FROM suppliers) PIVOT (AVG(rating) FOR country IN ('US', 'DE', 'TH', 'UK')) ORDER BY city;
