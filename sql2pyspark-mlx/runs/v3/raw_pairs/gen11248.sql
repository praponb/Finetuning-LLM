SELECT id, country, rating, city FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating) AS pos FROM suppliers WHERE rating <= 2.6) AS ranked WHERE pos <= 3;
