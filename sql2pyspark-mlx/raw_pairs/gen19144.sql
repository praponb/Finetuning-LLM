SELECT id, city FROM suppliers WHERE supplier_name LIKE '%Traders%' UNION SELECT id, city FROM suppliers WHERE country = 'US'
