SELECT city, rating FROM suppliers WHERE UPPER(TRIM(supplier_name)) = 'LION CITY IMPORTS' OR city IN ('New York', 'Berlin', 'Bangkok') ORDER BY rating;
