SELECT country, supplier_name FROM suppliers WHERE supplier_name LIKE 'Pa%' EXCEPT SELECT country, supplier_name FROM suppliers WHERE country IN ('JP', 'SG', 'TH') ORDER BY country;
