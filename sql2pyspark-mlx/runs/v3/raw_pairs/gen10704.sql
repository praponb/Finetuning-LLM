SELECT * FROM (SELECT id, country, rating, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS pos FROM suppliers WHERE rating >= 4.0) AS t WHERE pos <= 2;
