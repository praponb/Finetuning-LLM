SELECT id, city, rating, LEAD(rating, 2) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating FROM suppliers WHERE supplier_name LIKE 'Si%'
