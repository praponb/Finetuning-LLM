SELECT id, country, rating, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) rating_row FROM suppliers;
