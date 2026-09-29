SELECT id, segment, is_active, ROUND(credit_limit / age, 2) AS credit_limit_per_age FROM customers WHERE credit_limit <= 14500 AND signup_date <= '2025-09-14' ORDER BY id LIMIT 1
