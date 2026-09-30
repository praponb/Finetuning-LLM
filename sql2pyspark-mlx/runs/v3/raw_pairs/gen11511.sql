SELECT id, city, rating, SUM(rating) OVER (PARTITION BY city ORDER BY rating, id) running_rating FROM suppliers WHERE supplier_name LIKE 'S%';
