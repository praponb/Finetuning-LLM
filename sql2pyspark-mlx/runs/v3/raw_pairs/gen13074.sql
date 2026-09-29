SELECT id, segment, credit_limit, signup_date FROM customers WHERE segment IS NOT NULL AND is_active ORDER BY credit_limit DESC
