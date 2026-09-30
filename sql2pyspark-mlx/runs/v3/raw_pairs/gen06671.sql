SELECT id, country FROM suppliers WHERE LOWER(TRIM(supplier_name)) = 'siam parts co' UNION SELECT id, country FROM suppliers WHERE rating >= 3.6;
