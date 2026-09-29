WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS pos FROM suppliers) SELECT id, country, rating, city FROM ranked WHERE pos <= 5
