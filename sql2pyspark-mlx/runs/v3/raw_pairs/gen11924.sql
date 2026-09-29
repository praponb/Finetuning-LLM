SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS row_num FROM suppliers WHERE city = 'Chiang Mai') AS r WHERE row_num <= 5 ORDER BY country ASC
