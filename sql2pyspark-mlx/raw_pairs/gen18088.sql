SELECT id, city, rating, NTILE(10) OVER (PARTITION BY city ORDER BY rating, id) AS quartile FROM suppliers WHERE supplier_name NOT LIKE '%Trad%' ORDER BY rating;
