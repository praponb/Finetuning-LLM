SELECT country FROM suppliers WHERE supplier_name LIKE '%Sup%' UNION SELECT country FROM sessions ORDER BY country DESC;
