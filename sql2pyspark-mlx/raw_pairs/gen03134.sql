SELECT 'supplier' AS record_type, city FROM suppliers WHERE UPPER(TRIM(supplier_name)) = 'LION CITY IMPORTS' UNION SELECT 'store' AS record_type, city FROM stores ORDER BY record_type;
