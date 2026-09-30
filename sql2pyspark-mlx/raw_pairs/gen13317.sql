SELECT 'session' AS record_type, country FROM sessions WHERE device = 'mobile' UNION SELECT 'product' AS record_type, brand FROM products;
