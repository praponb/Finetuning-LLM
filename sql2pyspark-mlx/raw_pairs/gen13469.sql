SELECT * FROM (SELECT is_active, country, credit_limit FROM customers) PIVOT (AVG(credit_limit) FOR country IN ('UK', 'TH', 'DE', 'US')) ORDER BY is_active
