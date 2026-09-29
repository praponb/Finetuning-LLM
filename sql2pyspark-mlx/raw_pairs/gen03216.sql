SELECT rating, country, supplier_name, CEIL(rating) rating_whole FROM suppliers WHERE supplier_name LIKE 'P%' ORDER BY 1 DESC
