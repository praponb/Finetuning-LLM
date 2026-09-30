SELECT id, country, rating, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS rating_row FROM suppliers;
