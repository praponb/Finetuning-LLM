SELECT is_active, segment, CAST(is_active AS INT) is_active_flag FROM customers WHERE credit_limit < 12000 OR country NOT IN ('DE', 'TH', 'UK')
