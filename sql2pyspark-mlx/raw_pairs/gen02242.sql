SELECT 'customer' record_type, country label FROM customers UNION SELECT 'product' record_type, brand label FROM products WHERE supplier_id IN (26, 28) ORDER BY record_type
