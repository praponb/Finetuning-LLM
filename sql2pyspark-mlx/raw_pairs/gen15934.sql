SELECT id, country, rating FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS row_num FROM suppliers WHERE country = 'JP') AS t WHERE row_num <= 1
