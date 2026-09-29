SELECT segment, city FROM customers INTERSECT SELECT segment, city FROM customers WHERE UPPER(name) LIKE '%IA %'
