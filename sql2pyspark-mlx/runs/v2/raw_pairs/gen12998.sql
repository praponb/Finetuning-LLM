SELECT city, MAX(credit_limit) - MIN(credit_limit) credit_limit_range, COUNT(*) cnt FROM customers WHERE email IS NULL AND is_active GROUP BY city HAVING credit_limit_range > 66
