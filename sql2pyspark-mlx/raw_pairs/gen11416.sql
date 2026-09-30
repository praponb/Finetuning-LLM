WITH Numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS pos FROM suppliers) SELECT id, country, rating, city FROM Numbered WHERE pos = 1
