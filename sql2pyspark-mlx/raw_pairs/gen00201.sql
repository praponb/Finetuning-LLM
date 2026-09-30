SELECT 'store' AS kind, opened_date FROM stores WHERE region = 'South' UNION ALL SELECT 'customer' AS kind, signup_date FROM customers WHERE is_active = TRUE;
