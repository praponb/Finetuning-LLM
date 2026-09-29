SELECT segment, COUNT(credit_limit) AS credit_limit_count FROM customers WHERE (age <= 48 AND is_active) OR signup_date > '2025-06-17' GROUP BY 1 ORDER BY credit_limit_count DESC, segment ASC LIMIT 3
