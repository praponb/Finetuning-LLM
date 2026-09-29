SELECT supplier_name, country FROM suppliers WHERE supplier_name NOT LIKE '%Sup%' UNION ALL SELECT supplier_name, country FROM suppliers WHERE country = 'SG' ORDER BY country;
