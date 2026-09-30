SELECT segment, is_active, MIN(credit_limit) AS min_credit_limit FROM customers WHERE is_active GROUP BY segment, is_active ORDER BY 3 DESC, segment;
