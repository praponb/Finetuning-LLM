SELECT COUNT(credit_limit) AS credit_limit_count FROM customers WHERE is_active = FALSE AND is_active AND name IS NOT NULL ORDER BY credit_limit_count DESC;
