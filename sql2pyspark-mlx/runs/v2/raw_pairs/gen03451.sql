SELECT id, rating FROM suppliers WHERE UPPER(supplier_name) LIKE '%IAM%' OR country = 'US'
