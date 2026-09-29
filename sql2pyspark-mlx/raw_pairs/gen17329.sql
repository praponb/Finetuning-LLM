SELECT signup_date, country FROM customers WHERE credit_limit > 15800 AND (email IS NOT NULL OR city IN ('New York', 'Bangkok', 'Tokyo')) ORDER BY country
