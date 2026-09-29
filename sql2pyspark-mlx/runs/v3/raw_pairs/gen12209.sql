SELECT * FROM (SELECT id, city, rating, supplier_name, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rn FROM suppliers WHERE supplier_name NOT LIKE '%Paci%') AS r WHERE rn <= 5
