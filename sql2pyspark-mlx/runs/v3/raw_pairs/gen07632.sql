SELECT * FROM suppliers WHERE rating >= 3.6 AND (city != 'London' OR supplier_name LIKE '%Wholesal%')
