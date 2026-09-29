WITH Ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rn FROM suppliers WHERE country = 'SG') SELECT id, country, rating, city FROM Ranked WHERE rn <= 3
