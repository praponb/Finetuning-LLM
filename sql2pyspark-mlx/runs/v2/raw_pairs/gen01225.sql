SELECT city, UPPER(TRIM(supplier_name)) AS supplier_name_normalized FROM suppliers WHERE rating BETWEEN 2.6 AND 3.6 AND country IN ('JP', 'TH', 'SG');
