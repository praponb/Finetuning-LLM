SELECT segment, name FROM customers WHERE is_active = TRUE UNION SELECT segment, name FROM customers WHERE city IN ('Bangkok', 'Berlin');
