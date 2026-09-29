SELECT id, country, rating FROM (SELECT id, country, rating, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) rn FROM suppliers WHERE rating >= 3.5) t WHERE rn <= 1 ORDER BY rating DESC;
