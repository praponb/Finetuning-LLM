SELECT ROUND(AVG(credit_limit), 2) AS avg_credit_limit FROM customers WHERE email IS NULL OR is_active ORDER BY avg_credit_limit
