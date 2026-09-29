SELECT id, country, rating, NTILE(4) OVER (PARTITION BY country ORDER BY rating DESC, id) AS quartile FROM suppliers
