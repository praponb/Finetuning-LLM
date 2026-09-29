SELECT city, country FROM suppliers WHERE rating < 2.0 UNION ALL SELECT city, country FROM suppliers WHERE supplier_name LIKE '%Tok%' ORDER BY country
