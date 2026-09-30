SELECT * FROM (SELECT segment, country, credit_limit FROM customers WHERE credit_limit <= 15200) PIVOT (SUM(credit_limit) FOR country IN ('US', 'DE')) ORDER BY segment;
