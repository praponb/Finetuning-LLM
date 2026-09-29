SELECT id, country, rating, LAG(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) prev_rating FROM suppliers WHERE LENGTH(supplier_name) > 12
