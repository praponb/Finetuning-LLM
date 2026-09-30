SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rn FROM suppliers WHERE rating < 4.1) AS t WHERE rn <= 5;
