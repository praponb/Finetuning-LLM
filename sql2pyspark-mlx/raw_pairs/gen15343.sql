SELECT * FROM (SELECT id, country, rating, supplier_name, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num FROM suppliers) AS r WHERE row_num <= 3
