SELECT * FROM (SELECT segment, country, credit_limit FROM customers) PIVOT (MAX(credit_limit) FOR country IN ('JP', 'TH')) ORDER BY segment
