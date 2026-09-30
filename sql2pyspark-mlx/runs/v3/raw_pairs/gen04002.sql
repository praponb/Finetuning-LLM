SELECT id, country, rating FROM (SELECT id, country, rating, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num FROM suppliers) AS ranked WHERE row_num <= 2;
