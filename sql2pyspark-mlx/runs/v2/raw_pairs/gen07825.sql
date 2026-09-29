SELECT * FROM (SELECT city, country, credit_limit FROM customers WHERE country IN ('TH', 'SG', 'US')) PIVOT (MAX(credit_limit) FOR country IN ('TH', 'DE', 'UK')) ORDER BY city;
